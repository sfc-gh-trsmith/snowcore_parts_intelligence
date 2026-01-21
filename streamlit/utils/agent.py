"""
Unified Cortex Agent client for UPIP Sourcing Assistant.
Supports VP (strategic), Procurement Manager (operational), and R&D Engineer (technical) personas.

Uses st.chat_input and st.chat_message for proper chat UX with session state history.
"""

from __future__ import annotations

import json
from typing import Any

import streamlit as st

# Database and agent configuration
DATABASE = "SNOWCORE_PARTS_INTELLIGENCE"
SCHEMA = "DATA_SCIENCE"
AGENT_NAME = "SOURCING_ASSISTANT"

# Persona-specific example questions
EXAMPLE_QUESTIONS = {
    "vp": [
        "What is the total projected savings from consolidation?",
        "Which suppliers have the highest risk?",
        "Show cross-BU synergy opportunities",
    ],
    "procurement": [
        "What is our current maverick spend?",
        "Which parts have the highest markup vs benchmark?",
        "Recommend suppliers for Valve category",
    ],
    "engineer": [
        "What are the FDA requirements for actuators?",
        "Find FDA-approved valve alternatives",
        "What is the biocompatibility standard for this part?",
    ],
}


def _get_thread(page_id: str) -> list[dict[str, str]]:
    """Get or create conversation thread for a page."""
    if "agent_threads" not in st.session_state:
        st.session_state.agent_threads = {}
    if page_id not in st.session_state.agent_threads:
        st.session_state.agent_threads[page_id] = []
    return st.session_state.agent_threads[page_id]


def _clear_thread(page_id: str):
    """Clear conversation thread for a page."""
    if "agent_threads" in st.session_state:
        st.session_state.agent_threads[page_id] = []


def query_agent(session, question: str, context: str | None = None) -> dict[str, Any]:
    """
    Call the unified Cortex Agent with a question.
    
    Args:
        session: Snowflake session
        question: User's question in natural language
        context: Optional context about user's role/intent
        
    Returns:
        Dict with 'response' (text) or 'error' (message)
    """
    try:
        import _snowflake
    except ImportError:
        # Running outside Snowflake - return mock response
        return _mock_agent_response(question)
    
    # Build the message with optional context
    user_content = question
    if context:
        user_content = f"[Context: {context}]\n\n{question}"
    
    payload = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": user_content}]
            }
        ],
        "stream": False
    }
    
    try:
        response = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/databases/{DATABASE}/schemas/{SCHEMA}/agents/{AGENT_NAME}:run",
            {},  # headers
            {},  # params
            payload,
            None,  # request_guid
            60000  # timeout_ms
        )
        
        # Parse response based on format
        if isinstance(response, (list, tuple)):
            status_code = response[0]
            response_body = response[2] if len(response) > 2 else response[1]
        else:
            status_code = response.get("status")
            response_body = response.get("content", "{}")
        
        if status_code == 200:
            result = json.loads(response_body) if isinstance(response_body, str) else response_body
            return _parse_agent_response(result)
        else:
            return {"error": f"API Error ({status_code}): {response_body}"}
            
    except Exception as e:
        return {"error": f"Agent request failed: {str(e)}"}


def _parse_agent_response(result: Any) -> dict[str, Any]:
    """Parse streaming event format response from Cortex Agent."""
    text_parts = []
    tool_results = []
    
    if isinstance(result, list):
        for event in result:
            event_type = event.get("event", "")
            data = event.get("data", {})
            
            if event_type == "text":
                text_parts.append(data.get("text", ""))
            elif event_type == "tool_result":
                tool_results.append(data.get("text", ""))
            elif event_type == "analyst_result":
                tool_results.append(data.get("text", ""))
            elif event_type == "error":
                return {"error": data.get("message", "Unknown error")}
    elif isinstance(result, dict):
        # Non-streaming response
        if "message" in result:
            text_parts.append(result["message"])
        elif "response" in result:
            text_parts.append(result["response"])
    
    response_text = "".join(text_parts)
    
    return {
        "response": response_text,
        "tool_results": tool_results if tool_results else None
    }


def _mock_agent_response(question: str) -> dict[str, Any]:
    """Mock response for local development outside Snowflake."""
    question_lower = question.lower()
    
    if "risk" in question_lower and "supplier" in question_lower:
        return {
            "response": "Based on the supplier risk assessment, SUP001 (Arctic Components) has a composite risk score of 0.12 (Low Risk) with excellent supply continuity at 0.92. This supplier is recommended for strategic partnership.",
            "tool_results": None
        }
    elif "maverick" in question_lower or "off-contract" in question_lower:
        return {
            "response": "Current maverick spend is approximately $1.2M (15% of total procurement). The highest maverick spend is with non-preferred suppliers SUP005 and SUP010. Recommend implementing contract compliance monitoring.",
            "tool_results": None
        }
    elif "consolidat" in question_lower:
        return {
            "response": "There are 6 active consolidation scenarios with total projected savings of $1.55M. The highest ROI scenario is 'NA Fastener Consolidation' (CONS001) at 533% ROI with projected savings of $285K.",
            "tool_results": None
        }
    elif "fda" in question_lower or "compliance" in question_lower:
        return {
            "response": "FDA compliance requires audit trails for firmware updates, electronic signatures, and verification of access controls. Parts must meet 21 CFR Part 11 requirements for electronic records.",
            "tool_results": None
        }
    else:
        return {
            "response": f"I can help you with questions about parts, suppliers, inventory, compliance, procurement, and consolidation scenarios. Your question was: '{question}'",
            "tool_results": None
        }


def render_agent_panel(session, persona_context: str | None = None):
    """
    Render the agent chat interface using st.chat_input and st.chat_message.
    
    Args:
        session: Snowflake session
        persona_context: Context string describing the user's role (vp, procurement, engineer)
    """
    st.divider()
    st.subheader("Ask the Assistant")
    
    # Use persona as page_id for thread separation
    page_id = persona_context or "default"
    thread = _get_thread(page_id)
    
    # Header row with clear button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.caption("Ask questions about parts, suppliers, compliance, and procurement.")
    with col2:
        if st.button("Clear", key=f"clear_chat_{page_id}", use_container_width=True):
            _clear_thread(page_id)
            st.rerun()
    
    # Show example questions based on persona
    if persona_context:
        persona_key = persona_context.lower()
        if persona_key in EXAMPLE_QUESTIONS:
            with st.expander("Example questions", expanded=False):
                for q in EXAMPLE_QUESTIONS[persona_key]:
                    if st.button(q, key=f"ex_{page_id}_{hash(q)}", use_container_width=True):
                        # Add example as user message and process it
                        _handle_user_message(q, thread, session, persona_context)
                        st.rerun()
    
    # Scrollable container for chat messages
    chat_container = st.container(height=400, border=True)
    
    # Render existing messages
    for msg in thread:
        with chat_container.chat_message(msg["role"]):
            st.markdown(msg["content"])
    
    # Chat input (pinned to bottom of page)
    if prompt := st.chat_input(
        "Ask about parts, suppliers, compliance...",
        key=f"agent_input_{page_id}"
    ):
        _handle_user_message(prompt, thread, session, persona_context)
        st.rerun()


def _handle_user_message(
    prompt: str,
    thread: list[dict[str, str]],
    session,
    persona_context: str | None
):
    """Process user message: add to thread, call agent, add response."""
    # Add user message to thread
    thread.append({"role": "user", "content": prompt})
    
    # Call agent
    result = query_agent(session, prompt, persona_context)
    
    # Build response content
    if "error" in result:
        response_content = f"**Error:** {result['error']}"
    else:
        response_content = result.get("response", "No response received.")
        
        # Append tool results if present
        if result.get("tool_results"):
            response_content += "\n\n**Tool Results:**\n"
            for tr in result["tool_results"]:
                response_content += f"```\n{tr}\n```\n"
    
    # Add assistant response to thread
    thread.append({"role": "assistant", "content": response_content})
