# vpn_check.py
"""
VPN Connectivity Checker for AMI BIHAR ANALYTICS
Detects if the user is connected to the corporate VPN before allowing DB access.
"""

import socket
import os
import time
from typing import Tuple, Optional
import streamlit as st


class VPNChecker:
    """
    Checks VPN connectivity by attempting to reach a known internal host
    that is ONLY accessible via VPN (e.g., a jump host, internal DNS, or the DB itself).
    """
    
    def __init__(
        self,
        vpn_test_host: str = None,      # e.g., "internal-jumphost.bsphcl.in" or DB host
        vpn_test_port: int = 5432,       # PostgreSQL port (or 22 for SSH jump host)
        timeout_seconds: int = 5,
        retry_attempts: int = 3
    ):
        # Priority: env var > constructor arg > default
        self.vpn_test_host = vpn_test_host or os.getenv("VPN_TEST_HOST", "your-db-host.internal")
        self.vpn_test_port = int(os.getenv("VPN_TEST_PORT", vpn_test_port))
        self.timeout = timeout_seconds
        self.retries = retry_attempts
        
    def is_vpn_connected(self) -> bool:
        """
        Attempts TCP connection to an internal host:port.
        Returns True if reachable (VPN is ON), False otherwise.
        """
        for attempt in range(self.retries):
            try:
                sock = socket.create_connection(
                    (self.vpn_test_host, self.vpn_test_port),
                    timeout=self.timeout
                )
                sock.close()
                return True
            except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError):
                if attempt < self.retries - 1:
                    time.sleep(1)
                continue
        return False
    
    def get_connection_status(self) -> Tuple[bool, Optional[str]]:
        """
        Returns (is_connected, status_message)
        """
        if self.is_vpn_connected():
            return True, f"✅ VPN Connected — Reachable: {self.vpn_test_host}:{self.vpn_test_port}"
        else:
            return False, (
                f"❌ VPN Disconnected — Cannot reach {self.vpn_test_host}:{self.vpn_test_port}\n\n"
                f"**Please connect to the corporate VPN** and refresh this page."
            )


# Singleton instance
_vpn_checker: Optional[VPNChecker] = None

def get_vpn_checker() -> VPNChecker:
    global _vpn_checker
    if _vpn_checker is None:
        _vpn_checker = VPNChecker()
    return _vpn_checker


def render_vpn_gate():
    """
    Streamlit component that blocks the entire app if VPN is not connected.
    Place this at the VERY TOP of app.py (before any DB calls).
    """
    checker = get_vpn_checker()
    is_connected, message = checker.get_connection_status()
    
    # Use Streamlit's session state to cache the check (avoid re-checking on every interaction)
    if "vpn_checked" not in st.session_state:
        st.session_state.vpn_checked = False
        st.session_state.vpn_connected = is_connected
    
    # Re-check if previously failed (user might have connected VPN and refreshed)
    if not st.session_state.vpn_connected:
        st.session_state.vpn_connected = is_connected
    
    if not st.session_state.vpn_connected:
        # BLOCK THE ENTIRE APP
        st.set_page_config(page_title="AMI BIHAR Analytics — VPN Required", layout="centered")
        
        st.markdown(
            """
            <div style="text-align: center; padding: 4rem 2rem;">
                <h1 style="color: #dc2626; font-size: 3rem;">🔒 VPN Required</h1>
                <p style="font-size: 1.25rem; color: #6b7280; margin-top: 1rem;">
                    The AMI BIHAR Analytics dashboard requires an active VPN connection
                    to access the secure database.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        st.error(message, icon="🚫")
        
        st.markdown("---")
        st.subheader("How to connect:")
        st.markdown("""
        1. **Launch your VPN client** (e.g., Cisco AnyConnect, FortiClient, OpenVPN)
        2. **Connect to the BSPHCL corporate network**
        3. **Refresh this page** (F5 or Ctrl+R)
        """)
        
        # Auto-refresh option
        st.markdown("---")
        if st.button("🔄 Check VPN Connection Again", type="primary", use_container_width=True):
            st.rerun()
        
        # Stop execution here — nothing below this will run
        st.stop()
    
    # If we get here, VPN is connected — show a subtle indicator
    st.sidebar.success("🔒 VPN Connected", icon="✅")
