# GitHub OAuth Service for Fly.io Deployment
# This replaces GoogleOAuthService

import asyncio
import json
import logging
import time
import uuid
from typing import Optional, Dict, Any
import httpx

logger = logging.getLogger("onbrain-ai-bot")


class GitHubOAuthService:
    """GitHub OAuth 2.0 Service for user authentication"""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str = "http://localhost:8000/github/callback",
    ) -> None:
        """
        Initialize GitHub OAuth Service
        
        Args:
            client_id: GitHub OAuth App Client ID
            client_secret: GitHub OAuth App Client Secret
            redirect_uri: Callback URL (e.g., https://your-app.fly.dev/github/callback)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.pending_flows: Dict[str, Dict[str, Any]] = {}
        
        # GitHub API endpoints
        self.auth_url = "https://github.com/login/oauth/authorize"
        self.token_url = "https://github.com/login/oauth/access_token"
        self.user_url = "https://api.github.com/user"
    
    def create_auth_url(self, telegram_id: int) -> str:
        """
        Create GitHub authorization URL
        
        Args:
            telegram_id: Telegram user ID
            
        Returns:
            Authorization URL to send to user
        """
        state = secrets.token_urlsafe(32)
        
        # Store state and telegram_id for verification
        self.pending_flows[state] = {
            "telegram_id": telegram_id,
            "created_at": time.time(),
        }
        
        # Clean up stale flows (older than 10 minutes)
        self.cleanup_stale_flows()
        
        # Build authorization URL
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": "user:email",  # Request email scope
            "state": state,
            "allow_signup": "true",
        }
        
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        auth_url = f"{self.auth_url}?{query_string}"
        
        logger.info(f"✅ Created GitHub auth URL for user {telegram_id}")
        return auth_url
    
    async def exchange_code_for_token(self, code: str, state: str) -> Optional[Dict[str, Any]]:
        """
        Exchange authorization code for access token
        
        Args:
            code: Authorization code from GitHub
            state: State parameter for CSRF protection
            
        Returns:
            Dict with access_token, user_data, or None if failed
        """
        # Verify state
        if state not in self.pending_flows:
            logger.error(f"❌ Invalid state parameter: {state}")
            return None
        
        flow_data = self.pending_flows.pop(state)
        telegram_id = flow_data["telegram_id"]
        
        try:
            async with httpx.AsyncClient() as client:
                # Step 1: Exchange code for token
                token_response = await client.post(
                    self.token_url,
                    params={
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "code": code,
                        "redirect_uri": self.redirect_uri,
                    },
                    headers={"Accept": "application/json"},
                    timeout=10.0,
                )
                
                if token_response.status_code != 200:
                    logger.error(
                        f"❌ Token exchange failed: {token_response.status_code} - "
                        f"{token_response.text}"
                    )
                    return None
                
                token_data = token_response.json()
                access_token = token_data.get("access_token")
                
                if not access_token:
                    logger.error(f"❌ No access token in response: {token_data}")
                    return None
                
                logger.info(f"✅ Got access token for user {telegram_id}")
                
                # Step 2: Get user profile
                user_response = await client.get(
                    self.user_url,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/vnd.github.v3+json",
                        "User-Agent": "OnBrain-AI-Bot",
                    },
                    timeout=10.0,
                )
                
                if user_response.status_code != 200:
                    logger.error(
                        f"❌ User fetch failed: {user_response.status_code} - "
                        f"{user_response.text}"
                    )
                    return None
                
                user_data = user_response.json()
                logger.info(f"✅ Got user profile: {user_data.get('login')}")
                
                return {
                    "access_token": access_token,
                    "telegram_id": telegram_id,
                    "github_username": user_data.get("login"),
                    "github_email": user_data.get("email"),
                    "github_avatar_url": user_data.get("avatar_url"),
                    "github_bio": user_data.get("bio"),
                    "github_company": user_data.get("company"),
                    "github_location": user_data.get("location"),
                    "github_blog": user_data.get("blog"),
                    "github_url": user_data.get("html_url"),
                }
        
        except asyncio.TimeoutError:
            logger.error(f"❌ Token exchange timeout for user {telegram_id}")
            return None
        except Exception as exc:
            logger.error(f"❌ Token exchange error for user {telegram_id}: {exc}")
            return None
    
    def cleanup_stale_flows(self, max_age: int = 600) -> None:
        """
        Remove flows older than max_age seconds (default 10 minutes)
        
        Args:
            max_age: Maximum age of flows in seconds
        """
        current_time = time.time()
        stale_states = [
            state
            for state, data in self.pending_flows.items()
            if current_time - data["created_at"] > max_age
        ]
        
        for state in stale_states:
            del self.pending_flows[state]
            logger.debug(f"🧹 Cleaned up stale flow: {state}")


import secrets
