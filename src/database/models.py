from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, Numeric, BigInteger, Index
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime, timedelta
from decimal import Decimal
import enum
import time


# Crear Base declarativa (versión moderna de SQLAlchemy)
Base = declarative_base()

class UserRole(enum.Enum):
    ADMIN = "admin"
    PREMIUM = "premium"
    FREE = "free"

class SubscriptionStatus(enum.Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    PENDING = "pending"

class PaymentStatus(enum.Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(String(50), unique=True, nullable=False)
    username = Column(String(100))
    first_name = Column(String(100))
    last_name = Column(String(100))
    role = Column(String(20), default=UserRole.FREE.value)
    is_admin = Column(Boolean, default=False)
    language = Column(String(10), default='en')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_activity = Column(DateTime, default=datetime.utcnow)
    
    # Relaciones
    subscriptions = relationship("Subscription", back_populates="user")
    payments = relationship("Payment", back_populates="user")
    admin_config = relationship("AdminConfig", back_populates="user", uselist=False)
    
    def get_active_subscription(self):
        """Obtiene la suscripción activa del usuario"""
        return next((sub for sub in self.subscriptions 
                    if sub.status == SubscriptionStatus.ACTIVE.value 
                    and sub.expires_at > datetime.utcnow()), None)
    
    def get_current_limits(self):
        """Obtiene los límites actuales del usuario"""
        if self.is_admin and self.admin_config:
            return {
                'max_recordings': self.admin_config.max_recordings,
                'max_monitoring': self.admin_config.max_monitoring,
                'unlimited': self.admin_config.unlimited_access
            }
        
        active_sub = self.get_active_subscription()
        if active_sub:
            return {
                'max_recordings': active_sub.plan.max_recordings,
                'max_monitoring': active_sub.plan.max_monitoring,
                'unlimited': False
            }
        
        # Límites por defecto para usuarios gratuitos
        return {
            'max_recordings': 2,
            'max_monitoring': 0,
            'unlimited': False
        }

class AdminConfig(Base):
    __tablename__ = 'admin_configs'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), unique=True)
    max_recordings = Column(Integer, default=50)
    max_monitoring = Column(Integer, default=50)
    unlimited_access = Column(Boolean, default=False)
    custom_permissions = Column(Text)  # JSON string
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", back_populates="admin_config")

class SubscriptionPlan(Base):
    __tablename__ = 'subscription_plans'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    price = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(3), default='USD')
    duration_days = Column(Integer, nullable=False)
    max_recordings = Column(Integer, nullable=False)
    max_monitoring = Column(Integer, nullable=False)
    stripe_price_id = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    subscriptions = relationship("Subscription", back_populates="plan")

class Subscription(Base):
    __tablename__ = 'subscriptions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    plan_id = Column(Integer, ForeignKey('subscription_plans.id'))
    stripe_subscription_id = Column(String(100))
    status = Column(String(20), default=SubscriptionStatus.PENDING.value)
    starts_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", back_populates="subscriptions")
    plan = relationship("SubscriptionPlan", back_populates="subscriptions")
    
    def is_active(self):
        return (self.status == SubscriptionStatus.ACTIVE.value and 
                self.expires_at > datetime.utcnow())

class Payment(Base):
    __tablename__ = 'payments'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    subscription_id = Column(Integer, ForeignKey('subscriptions.id'), nullable=True)
    stripe_payment_intent_id = Column(String(100))
    amount = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(3), default='USD')
    status = Column(String(20), default=PaymentStatus.PENDING.value)
    payment_metadata = Column(Text)  # Renombrado para evitar conflicto con 'metadata'
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", back_populates="payments")

class RoomCache(Base):
    """Modelo para cache de room_ids de TikTok"""
    __tablename__ = 'room_cache'
    
    username = Column(String(100), primary_key=True, index=True)
    room_id = Column(String(50), nullable=False)
    last_updated = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    is_live = Column(Boolean, default=True, index=True)
    failed_attempts = Column(Integer, default=0)
    last_failed = Column(BigInteger, default=0)