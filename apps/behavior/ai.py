"""Gemini-based helper utilities for generating action plans."""
from __future__ import annotations

import logging
import os
from typing import Iterable

try:
    from google import genai
    from google.genai import types
except ImportError:  # pragma: no cover - optional dependency
    genai = None  # type: ignore
    types = None  # type: ignore

logger = logging.getLogger(__name__)

_CLIENT: genai.Client | None = None  # type: ignore
_MODEL_NAME = "gemini-2.5-flash-lite"


def _get_client() -> "genai.Client":
    """Return a cached Gemini client instance."""
    global _CLIENT
    if genai is None:
        raise RuntimeError("google-genai is not installed. Run `pip install google-genai`." )
    if _CLIENT is not None:
        return _CLIENT
    api_key = "AIzaSyBV1NM1C3EbUgzZPenmDgUOh7QS8h6-2bk"
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is not configured.")
    _CLIENT = genai.Client(api_key=api_key)
    return _CLIENT


def _iter_text_parts(chunks: Iterable["types.GenerateContentResponse"]) -> Iterable[str]:  # type: ignore[name-defined]
    """Yield plain text fragments from a streaming Gemini response."""
    for chunk in chunks:
        candidates = getattr(chunk, "candidates", None)
        if not candidates:
            continue
        content = getattr(candidates[0], "content", None)
        if not content:
            continue
        for part in getattr(content, "parts", []) or []:
            text = getattr(part, "text", None)
            if text:
                yield text


def build_prompt(profile: dict) -> str:
    """Craft the instruction payload for Gemini based on the student profile."""
    student_line = (
        f"Student: {profile.get('student_name', 'Unknown')} (ESIS {profile.get('esis', 'N/A')})."
    )
    homeroom = profile.get("homeroom") or "Unknown"
    summary = profile.get("summary", {})
    incidents = profile.get("incidents", [])
    lines = [
        student_line,
        f"Homeroom / Class: {homeroom}.",
        f"Incident summary: total {summary.get('total', 0)}, minor {summary.get('minor', 0)}, major {summary.get('major', 0)}."
    ]

    if incidents:
        lines.append("Recent incidents (most recent first):")
        for idx, inc in enumerate(incidents[:6], 1):
            lines.append(
                f"  {idx}. {inc.get('date')} | Grade {inc.get('grade')} | {inc.get('place')} | Action: {inc.get('action')} | Description: {inc.get('desc')}"
            )
    else:
        lines.append("No behaviour incidents recorded.")

    def _append_section(title: str, items: Iterable[dict], formatter, limit: int = 4):
        items = list(items)[:limit]
        if not items:
            return
        lines.append('')
        lines.append(f"{title}:")
        for item in items:
            lines.append(formatter(item))

    _append_section(
        "Parent meetings",
        profile.get("parent_meetings_full", []),
        lambda pm: (
            f"  - {pm['date']} with {pm.get('parent_name') or 'N/A'} | Attendees: {pm.get('attended_by') or 'N/A'} | Concerns: {pm.get('school_concerns') or pm.get('parent_concerns') or 'N/A'} | Next steps: {pm.get('agreed_next_steps') or 'N/A'}"
        ),
    )
    _append_section(
        "Student statements",
        profile.get("student_statements_full", []),
        lambda st: f"  - {st['date']} at {st.get('location') or 'N/A'} | Summary: {st.get('statement') or 'N/A'}",
    )
    _append_section(
        "Staff statements",
        profile.get("staff_statements_full", []),
        lambda st: f"  - {st['date']} by {st.get('staff_name') or 'Unknown'} | Details: {st.get('details') or 'N/A'}",
    )
    _append_section(
        "Safeguarding concerns",
        profile.get("safeguarding_full", []),
        lambda sg: f"  - {sg['report_date']} by {sg.get('reporting_name')} ({sg.get('reporting_role')}) | {sg.get('description') or 'N/A'}",
    )
    _append_section(
        "Suspensions",
        profile.get("suspensions_full", []),
        lambda sp: f"  - {sp['date_of_suspension']} ({sp.get('duration')}) | Reason: {sp.get('reason') or 'N/A'} | Plan: {sp.get('behavior_plan') or 'N/A'}",
    )
    _append_section(
        "Counseling sessions",
        profile.get("counseling_sessions_full", []),
        lambda cs: f"  - {cs['session_date']} led by {cs.get('counselors') or 'Unknown'} | Focus: {cs.get('focus_summary')} | Notes: {cs.get('summary_of_progress') or 'N/A'}",
    )

    _append_section(
        "Additional interventions",
        profile.get("behavior_contracts_full", []),
        lambda bc: f"  - {bc['date']} grade {bc.get('grade') or 'N/A'} | Consequences: {bc.get('consequences') or 'N/A'} | Notes: {bc.get('notes') or 'N/A'}",
    )

    dynamic_forms = profile.get("dynamic_form_titles", [])
    if dynamic_forms:
        lines.append('')
        lines.append("Other submitted forms:")
        for entry in dynamic_forms[:6]:
            lines.append(f"  - {entry}")

    lines.append('')
    lines.append('Respond with exactly five lines using this template:')
    lines.append('Overview: one sentence (<=25 words) summarising the behaviour context.')
    lines.append('Key Trends: up to three short phrases separated by semicolons.')
    lines.append('Immediate Supports: up to three short actions separated by semicolons.')
    lines.append('Next Steps: up to three short actions separated by semicolons.')
    lines.append('Collaboration: up to three short actions separated by semicolons.')
    lines.append('Keep the entire reply under 130 words. Use plain text only - no headings, markdown, or extra commentary before or after these lines.')
    return '\n'.join(lines)


def generate_action_plan(profile: dict) -> str:
    """Call Gemini and return the generated action plan text."""
    client = _get_client()
    prompt = build_prompt(profile)
    if types is None:
        raise RuntimeError("google-genai library is unavailable.")
    contents = [
        types.Content(
            role="user",
            parts=[types.Part.from_text(text=prompt)],
        )
    ]
    tools = [types.Tool(code_execution=types.ToolCodeExecution)]
    config = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_budget=0),
        tools=tools,
    )
    logger.debug("Requesting Gemini action plan for ESIS %s", profile.get("esis"))
    chunks = client.models.generate_content_stream(
        model=_MODEL_NAME,
        contents=contents,
        config=config,
    )
    text_parts = list(_iter_text_parts(chunks))
    result = "".join(text_parts).strip()
    if not result:
        raise RuntimeError("No response received from Gemini.")
    return result
