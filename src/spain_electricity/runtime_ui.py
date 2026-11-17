"""One-click data-source selection and live connection feedback for the Energy Desk."""

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from uuid import uuid4

import streamlit as st
from pyspark.sql import SparkSession

from spain_electricity import views
from spain_electricity.config import DataPaths
from spain_electricity.live_runtime import LiveService, LiveStatus


@st.cache_resource
def live_service(_spark: SparkSession, root: str) -> LiveService:
    """Share one idle controller per data root; queries start only on an explicit live selection."""
    return LiveService(_spark, DataPaths(Path(root)))


@st.cache_data(ttl=60, show_spinner=False)
def _days(_spark: SparkSession, root: str) -> list[date]:
    return views.historical_days(_spark, DataPaths(Path(root)))


@dataclass
class SourceSelection:
    """The viewer's source choice; it never mutates the shared history/model paths."""

    paths: DataPaths
    historical_day: date | None
    real_time: bool
    client_id: str
    service: LiveService


def source_selector(spark: SparkSession, paths: DataPaths) -> SourceSelection:
    """Render an immediate source switch and start live work asynchronously when requested."""
    if "desk_viewer_id" not in st.session_state:
        st.session_state.desk_viewer_id = uuid4().hex
    source = st.radio(
        "Data source", ["Historical", "Replay", "Real time"], horizontal=True, key="desk_source"
    )
    real_time = source == "Real time"
    service = live_service(spark, str(paths.root))
    client_id = st.session_state.desk_viewer_id
    service.request(client_id, real_time)
    day = None
    if real_time:
        st.caption("Connects automatically · captures every 5 min. Historical data stays separate.")
    elif source == "Historical":
        days = _days(spark, str(paths.root))
        if days:
            day = st.selectbox(
                "Historical day",
                days,
                format_func=lambda value: value.strftime("%d %b %Y"),
                key="desk_historical_day",
            )
        st.caption("Select a downloaded day. Switching back pauses your live-data request.")
    else:
        st.caption("Shows the replay stream output. Start the replay and streaming scripts to update it.")
    return SourceSelection(
        paths.with_stream("live" if real_time else "replay") if source != "Historical" else paths,
        day,
        real_time,
        client_id,
        service,
    )


def connection_status(selection: SourceSelection) -> LiveStatus:
    """Heartbeat while viewing live; display startup and recoverable errors without terminal instructions."""
    status = selection.service.request(selection.client_id, selection.real_time)
    if not selection.real_time:
        return status
    if status.error:
        st.error(
            "The live connection stopped. Your historical data is safe; retry the connection or switch views."
        )
        if st.button("Retry connection", key="desk_retry_live"):
            selection.service.request(selection.client_id, True, retry=True)
            st.rerun(scope="fragment")
        with st.expander("Connection details"):
            st.text(status.error)
    elif not status.ready:
        st.info(
            "Connecting to current data and preparing the live view. "
            "The first observations normally appear after processing the first capture."
        )
    else:
        stamp = (
            status.last_capture.strftime("%H:%M:%S UTC") if status.last_capture else "restoring saved state"
        )
        st.caption(f"Live connection active · last capture: {stamp} · automatic 5-minute updates")
    if status.reference_warning:
        st.warning(
            "The gas reference could not be updated. "
            "Other live sources continue; unavailable values stay blank."
        )
    return status
