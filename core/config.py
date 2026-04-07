"""
Configuration module for Supershyft backend.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """
    Application settings loaded from environment variables.
    """
    
    # Application settings
    APP_NAME: str = os.getenv("APP_NAME", "Supershyft API")
    APP_VERSION: str = os.getenv("APP_VERSION", "1.0.0")
    APP_ENVIRONMENT: str = os.getenv("APP_ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # Server settings
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    # Database settings
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    DATABASE_POOL_SIZE: int = int(os.getenv("DATABASE_POOL_SIZE", "5"))
    DATABASE_MAX_OVERFLOW: int = int(os.getenv("DATABASE_MAX_OVERFLOW", "10"))
    
    # JWT settings
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = int(
        os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "30")
    )
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = int(
        os.getenv("JWT_REFRESH_TOKEN_EXPIRE_DAYS", "7")
    )
    
    # CORS settings
    CORS_ORIGINS: list = [
        origin.strip()
        for origin in os.getenv(
            "CORS_ORIGINS",
            "http://localhost:3000,http://localhost:5173,"
            "http://127.0.0.1:5500,http://localhost:5500",
        ).split(",")
        if origin.strip()
    ]
    
    # Logging settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # Security settings
    BCRYPT_ROUNDS: int = int(os.getenv("BCRYPT_ROUNDS", "12"))
    
    # OTP settings
    OTP_LOG_TO_TERMINAL: bool = os.getenv("OTP_LOG_TO_TERMINAL", "True").lower() == "true"
    ALLOW_BYPASS_OTP: bool = os.getenv(
        "ALLOW_BYPASS_OTP",
        os.getenv("allow_bypass_otp", "False"),
    ).lower() == "true"

    # Media upload settings
    MEDIA_ROOT: str = os.getenv("MEDIA_ROOT", "/var/www/backend/media")
    MEDIA_BASE_URL: str = os.getenv("MEDIA_BASE_URL", "http://localhost:8000/media")
    USER_PROFILE_PHOTO_MAX_MB: int = int(os.getenv("USER_PROFILE_PHOTO_MAX_MB", "2"))
    ORG_LOGO_MAX_MB: int = int(os.getenv("ORG_LOGO_MAX_MB", "5"))

    # Metsights integration settings
    METSIGHTS_BASE_URL: str = os.getenv("METSIGHTS_BASE_URL", "https://api.metsights.com")
    METSIGHTS_API_KEY: str = os.getenv("METSIGHTS_API_KEY", "")
    METSIGHTS_TIMEOUT_SECONDS: int = int(os.getenv("METSIGHTS_TIMEOUT_SECONDS", "15"))

    # Razorpay (server-side secret; never expose RAZORPAY_KEY_SECRET to clients)
    RAZORPAY_KEY_ID: str = os.getenv("RAZORPAY_KEY_ID", "")
    RAZORPAY_KEY_SECRET: str = os.getenv("RAZORPAY_KEY_SECRET", "")
    
    @classmethod
    def validate(cls) -> None:
        """
        Validate that all required settings are present.
        
        Raises:
            ValueError: If any required setting is missing.
        """
        required_settings = [
            ("DATABASE_URL", cls.DATABASE_URL),
            ("JWT_SECRET_KEY", cls.JWT_SECRET_KEY),
        ]
        
        missing = []
        for name, value in required_settings:
            if not value:
                missing.append(name)
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
    
    @classmethod
    def is_production(cls) -> bool:
        """Check if running in production environment."""
        return cls.APP_ENVIRONMENT.lower() == "production"
    
    @classmethod
    def is_development(cls) -> bool:
        """Check if running in development environment."""
        return cls.APP_ENVIRONMENT.lower() == "development"
    
    @classmethod
    def is_testing(cls) -> bool:
        """Check if running in test environment."""
        return cls.APP_ENVIRONMENT.lower() == "testing"


# Create a singleton instance
settings = Settings()
