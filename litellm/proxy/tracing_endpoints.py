"""
Agent tracing endpoints. Thin wrappers over `TraceReceiver`: auth -> tenant/scope -> one call.

POST /v1/traces                              OTLP/HTTP trace export (protobuf or JSON)
GET  /v1/traces                              TracePage
GET  /v1/traces/{trace_id}                   Trace
GET  /v1/traces/{trace_id}/spans/{span_id}   SpanDetail
"""

import time
from collections.abc import Mapping
from typing import Annotated, Final

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from litellm.constants import OTLP_RETRY_AFTER_SECONDS
from litellm.proxy._types import LitellmUserRoles, UserAPIKeyAuth
from litellm.proxy.auth.user_api_key_auth import user_api_key_auth
from litellm.tracing import (
    Tenant,
    TraceReceiver,
    TracingPayloadTooLargeError,
)
from litellm.tracing.decode import InvalidOTLPPayloadError, encode_otlp_response
from litellm.tracing.types import SpanDetail, SpanErrorPage, Trace, TracePage, TraceScope

router = APIRouter(tags=["agent tracing"])  # mutable-ok: FastAPI copies the mutable tags list

MS_PER_DAY: Final = 24 * 60 * 60 * 1000
_ADMIN_ROLES: Final = (LitellmUserRoles.PROXY_ADMIN, LitellmUserRoles.PROXY_ADMIN_VIEW_ONLY)

receiver: TraceReceiver | None = None


def get_receiver() -> TraceReceiver:
    if receiver is None:
        raise HTTPException(
            status_code=501,
            detail="Agent tracing is not enabled. Set `tracing:` in general_settings and CLICKHOUSE_URL.",
        )
    return receiver


def tenant_for(user_api_key_dict: UserAPIKeyAuth) -> Tenant:
    return Tenant(
        team_id=user_api_key_dict.team_id or "",
        api_key_hash=user_api_key_dict.token or "",
        org_id=user_api_key_dict.org_id or "",
    )


def scope_for(user_api_key_dict: UserAPIKeyAuth) -> TraceScope:
    """Admins see everything; team members see their team; team-less keys see their own traces."""
    if user_api_key_dict.user_role in _ADMIN_ROLES:
        return TraceScope(team_ids=(), api_key_hash="")
    if user_api_key_dict.team_id:
        return TraceScope(team_ids=(user_api_key_dict.team_id,), api_key_hash="")
    if not user_api_key_dict.token:
        raise HTTPException(status_code=403, detail="Not allowed to view agent traces")
    return TraceScope(team_ids=("",), api_key_hash=user_api_key_dict.token)


def otlp_error_response(
    request: Request, status_code: int, message: str, headers: Mapping[str, str] | None = None
) -> Response | None:
    if request.url.path != "/v1/traces" or request.method != "POST":
        return None
    body, media_type = encode_otlp_response(request.headers.get("content-type"), message[:1024])
    return Response(content=body, status_code=status_code, media_type=media_type, headers=headers)


def _otlp_error(content_type: str | None, status_code: int, message: str, retry: bool = False) -> Response:
    body, media_type = encode_otlp_response(content_type, message)
    return Response(
        content=body,
        status_code=status_code,
        media_type=media_type,
        headers={"Retry-After": str(OTLP_RETRY_AFTER_SECONDS)} if retry else None,
    )


@router.post("/v1/traces", include_in_schema=False)
async def ingest_otlp_traces(
    request: Request,
    user_api_key_dict: Annotated[UserAPIKeyAuth, Depends(user_api_key_auth)],
) -> Response:
    content_type: Final = request.headers.get("content-type")
    if user_api_key_dict.user_role == LitellmUserRoles.PROXY_ADMIN_VIEW_ONLY:
        return _otlp_error(content_type, 403, "Not allowed to ingest agent traces")
    try:
        tracing: Final = get_receiver()
        await tracing.ingest(
            body=request.stream(),
            content_type=content_type,
            content_encoding=request.headers.get("content-encoding"),
            tenant=tenant_for(user_api_key_dict),
        )
    except TracingPayloadTooLargeError as e:
        return _otlp_error(content_type, 413, str(e))
    except InvalidOTLPPayloadError as error:
        return _otlp_error(content_type, 400, str(error))
    except RuntimeError:
        return _otlp_error(content_type, 503, "Trace ingestion is temporarily unavailable", retry=True)
    except HTTPException as error:
        return _otlp_error(content_type, error.status_code, str(error.detail))
    body, media_type = encode_otlp_response(content_type)
    return Response(content=body, media_type=media_type)


@router.get("/v1/traces", response_model=None)
async def list_agent_traces(
    user_api_key_dict: Annotated[UserAPIKeyAuth, Depends(user_api_key_auth)],
    start_ms: Annotated[int | None, Query(description="Window start, unix ms. Default: 24h ago")] = None,
    end_ms: Annotated[int | None, Query(description="Window end, unix ms. Default: now")] = None,
    cursor: Annotated[str | None, Query()] = None,
) -> TracePage:
    now_ms: Final = int(time.time() * 1000)
    try:
        return await get_receiver().list_traces(
            scope=scope_for(user_api_key_dict),
            start_ms=start_ms if start_ms is not None else now_ms - MS_PER_DAY,
            end_ms=end_ms if end_ms is not None else now_ms,
            cursor=cursor,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/v1/traces/{trace_id}", response_model=None)
async def get_agent_trace(
    trace_id: str,
    user_api_key_dict: Annotated[UserAPIKeyAuth, Depends(user_api_key_auth)],
    trace_ref: Annotated[str, Query()] = "",
) -> Trace:
    trace: Final = await get_receiver().get_trace(trace_id, scope_for(user_api_key_dict), trace_ref)
    if trace is None:
        raise HTTPException(status_code=404, detail=f"Trace {trace_id} not found")
    return trace


@router.get("/v1/traces/{trace_id}/spans/{span_id}", response_model=None)
async def get_agent_trace_span(
    trace_id: str,
    span_id: str,
    user_api_key_dict: Annotated[UserAPIKeyAuth, Depends(user_api_key_auth)],
    trace_ref: Annotated[str, Query()] = "",
) -> SpanDetail:
    span: Final = await get_receiver().get_span(trace_id, span_id, scope_for(user_api_key_dict), trace_ref)
    if span is None:
        raise HTTPException(status_code=404, detail=f"Span {span_id} not found")
    return span


@router.get("/v1/traces/{trace_id}/spans/{span_id}/error", response_model=SpanErrorPage)
async def get_agent_trace_span_error(
    trace_id: str,
    span_id: str,
    user_api_key_dict: Annotated[UserAPIKeyAuth, Depends(user_api_key_auth)],
    trace_ref: Annotated[str, Query()] = "",
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> SpanErrorPage:
    try:
        page: Final = await get_receiver().get_span_error(
            trace_id, span_id, scope_for(user_api_key_dict), trace_ref, cursor
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    if page is None:
        raise HTTPException(status_code=404, detail="Span diagnostic not found or no longer available")
    return page
