"""
Utilidades de seguridad para el pipeline de datos.
Incluye encriptación, validación, auditoría y gestión de acceso.
"""

import os
import hashlib
import hmac
import secrets
import base64
import json
from typing import Dict, Any, Optional, List, Union, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
from functools import wraps

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    import jwt
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logging.warning("Librerías de criptografía no disponibles")

from src.utils.logging import get_logger, log_execution_time

logger = get_logger(__name__)

class SecurityLevel(Enum):
    """Niveles de seguridad para datos."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

class AccessLevel(Enum):
    """Niveles de acceso para usuarios."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    OWNER = "owner"

@dataclass
class SecurityContext:
    """Contexto de seguridad para operaciones."""
    user_id: str
    session_id: str
    access_level: AccessLevel
    permissions: List[str] = field(default_factory=list)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    
    def is_expired(self) -> bool:
        """Verificar si el contexto ha expirado."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at
    
    def has_permission(self, permission: str) -> bool:
        """Verificar si tiene un permiso específico."""
        return permission in self.permissions or self.access_level == AccessLevel.OWNER
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario."""
        return {
            'user_id': self.user_id,
            'session_id': self.session_id,
            'access_level': self.access_level.value,
            'permissions': self.permissions,
            'ip_address': self.ip_address,
            'user_agent': self.user_agent,
            'timestamp': self.timestamp.isoformat(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None
        }

@dataclass
class DataClassification:
    """Clasificación de seguridad para datos."""
    level: SecurityLevel
    categories: List[str] = field(default_factory=list)
    retention_days: Optional[int] = None
    encryption_required: bool = True
    audit_required: bool = True
    pii_fields: List[str] = field(default_factory=list)
    
    def requires_encryption(self) -> bool:
        """Verificar si requiere encriptación."""
        return self.encryption_required or self.level in [SecurityLevel.CONFIDENTIAL, SecurityLevel.RESTRICTED]
    
    def requires_audit(self) -> bool:
        """Verificar si requiere auditoría."""
        return self.audit_required or self.level in [SecurityLevel.CONFIDENTIAL, SecurityLevel.RESTRICTED]

class EncryptionManager:
    """Gestor de encriptación para datos sensibles."""
    
    def __init__(self, master_key: Optional[str] = None):
        if not CRYPTO_AVAILABLE:
            logger.warning("Criptografía no disponible, usando codificación base64")
            self._fernet = None
        else:
            self._master_key = master_key or self._generate_master_key()
            self._fernet = Fernet(self._master_key.encode() if isinstance(self._master_key, str) else self._master_key)
    
    def _generate_master_key(self) -> bytes:
        """Generar clave maestra."""
        if CRYPTO_AVAILABLE:
            return Fernet.generate_key()
        else:
            return base64.urlsafe_b64encode(secrets.token_bytes(32))
    
    @log_execution_time
    def encrypt(self, data: Union[str, bytes], context: Optional[SecurityContext] = None) -> str:
        """
        Encriptar datos.
        
        Args:
            data: Datos a encriptar
            context: Contexto de seguridad
            
        Returns:
            Datos encriptados en base64
        """
        try:
            if isinstance(data, str):
                data = data.encode('utf-8')
            
            if self._fernet and CRYPTO_AVAILABLE:
                encrypted = self._fernet.encrypt(data)
                result = base64.urlsafe_b64encode(encrypted).decode('utf-8')
            else:
                # Fallback: solo codificación base64 (NO es seguro para producción)
                logger.warning("Usando codificación base64 - NO seguro para producción")
                result = base64.urlsafe_b64encode(data).decode('utf-8')
            
            if context:
                logger.info(f"Datos encriptados por usuario {context.user_id}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error encriptando datos: {e}")
            raise SecurityError(f"Error en encriptación: {e}")
    
    @log_execution_time
    def decrypt(self, encrypted_data: str, context: Optional[SecurityContext] = None) -> bytes:
        """
        Desencriptar datos.
        
        Args:
            encrypted_data: Datos encriptados en base64
            context: Contexto de seguridad
            
        Returns:
            Datos desencriptados
        """
        try:
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode('utf-8'))
            
            if self._fernet and CRYPTO_AVAILABLE:
                result = self._fernet.decrypt(encrypted_bytes)
            else:
                # Fallback: solo decodificación base64
                logger.warning("Usando decodificación base64 - datos no estaban realmente encriptados")
                result = encrypted_bytes
            
            if context:
                logger.info(f"Datos desencriptados por usuario {context.user_id}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error desencriptando datos: {e}")
            raise SecurityError(f"Error en desencriptación: {e}")
    
    def encrypt_field(self, value: Any, field_name: str) -> str:
        """Encriptar un campo específico."""
        if value is None:
            return None
        
        field_data = {
            'field': field_name,
            'value': value,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return self.encrypt(json.dumps(field_data))
    
    def decrypt_field(self, encrypted_value: str, expected_field: str) -> Any:
        """Desencriptar un campo específico."""
        if not encrypted_value:
            return None
        
        try:
            decrypted_data = self.decrypt(encrypted_value)
            field_data = json.loads(decrypted_data.decode('utf-8'))
            
            if field_data.get('field') != expected_field:
                raise SecurityError(f"Campo esperado '{expected_field}' no coincide")
            
            return field_data.get('value')
            
        except Exception as e:
            logger.error(f"Error desencriptando campo '{expected_field}': {e}")
            raise

class HashManager:
    """Gestor de hashing para integridad de datos."""
    
    @staticmethod
    def hash_data(data: Union[str, bytes], algorithm: str = 'sha256') -> str:
        """
        Generar hash de datos.
        
        Args:
            data: Datos a hashear
            algorithm: Algoritmo de hash (sha256, sha512, md5)
            
        Returns:
            Hash en hexadecimal
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        if algorithm == 'sha256':
            return hashlib.sha256(data).hexdigest()
        elif algorithm == 'sha512':
            return hashlib.sha512(data).hexdigest()
        elif algorithm == 'md5':
            return hashlib.md5(data).hexdigest()
        else:
            raise ValueError(f"Algoritmo de hash no soportado: {algorithm}")
    
    @staticmethod
    def verify_hash(data: Union[str, bytes], expected_hash: str, algorithm: str = 'sha256') -> bool:
        """Verificar integridad de datos con hash."""
        actual_hash = HashManager.hash_data(data, algorithm)
        return hmac.compare_digest(actual_hash, expected_hash)
    
    @staticmethod
    def hash_password(password: str, salt: Optional[str] = None) -> Tuple[str, str]:
        """
        Hashear contraseña con sal.
        
        Args:
            password: Contraseña a hashear
            salt: Sal opcional (se genera si no se proporciona)
            
        Returns:
            Tupla (hash, salt)
        """
        if salt is None:
            salt = secrets.token_hex(32)
        
        # Usar PBKDF2 para contraseñas
        if CRYPTO_AVAILABLE:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt.encode('utf-8'),
                iterations=100000,
            )
            key = kdf.derive(password.encode('utf-8'))
            password_hash = base64.urlsafe_b64encode(key).decode('utf-8')
        else:
            # Fallback simple (NO seguro para producción)
            password_hash = hashlib.sha256((password + salt).encode('utf-8')).hexdigest()
        
        return password_hash, salt
    
    @staticmethod
    def verify_password(password: str, password_hash: str, salt: str) -> bool:
        """Verificar contraseña."""
        computed_hash, _ = HashManager.hash_password(password, salt)
        return hmac.compare_digest(computed_hash, password_hash)

class TokenManager:
    """Gestor de tokens JWT para autenticación."""
    
    def __init__(self, secret_key: str, algorithm: str = 'HS256'):
        self.secret_key = secret_key
        self.algorithm = algorithm
    
    def generate_token(self, context: SecurityContext, expires_in_hours: int = 24) -> str:
        """
        Generar token JWT.
        
        Args:
            context: Contexto de seguridad
            expires_in_hours: Horas hasta expiración
            
        Returns:
            Token JWT
        """
        if not CRYPTO_AVAILABLE:
            logger.warning("JWT no disponible, generando token simple")
            return base64.urlsafe_b64encode(
                json.dumps(context.to_dict()).encode('utf-8')
            ).decode('utf-8')
        
        payload = {
            'user_id': context.user_id,
            'session_id': context.session_id,
            'access_level': context.access_level.value,
            'permissions': context.permissions,
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(hours=expires_in_hours)
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    def verify_token(self, token: str) -> Optional[SecurityContext]:
        """
        Verificar y decodificar token JWT.
        
        Args:
            token: Token JWT
            
        Returns:
            Contexto de seguridad o None si es inválido
        """
        try:
            if not CRYPTO_AVAILABLE:
                # Fallback para token simple
                data = json.loads(
                    base64.urlsafe_b64decode(token.encode('utf-8')).decode('utf-8')
                )
                return SecurityContext(
                    user_id=data['user_id'],
                    session_id=data['session_id'],
                    access_level=AccessLevel(data['access_level']),
                    permissions=data.get('permissions', [])
                )
            
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            return SecurityContext(
                user_id=payload['user_id'],
                session_id=payload['session_id'],
                access_level=AccessLevel(payload['access_level']),
                permissions=payload.get('permissions', []),
                expires_at=datetime.fromtimestamp(payload['exp'])
            )
            
        except Exception as e:
            logger.warning(f"Token inválido: {e}")
            return None

class AuditLogger:
    """Logger de auditoría para operaciones de seguridad."""
    
    def __init__(self, log_file: Optional[str] = None):
        self.audit_logger = get_logger(f"{__name__}.audit")
        self.log_file = log_file
    
    def log_access(self, context: SecurityContext, resource: str, action: str, 
                   success: bool, details: Optional[Dict[str, Any]] = None):
        """Registrar acceso a recursos."""
        audit_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': 'access',
            'user_id': context.user_id,
            'session_id': context.session_id,
            'resource': resource,
            'action': action,
            'success': success,
            'ip_address': context.ip_address,
            'user_agent': context.user_agent,
            'details': details or {}
        }
        
        self.audit_logger.info(f"AUDIT: {json.dumps(audit_entry)}")
    
    def log_data_access(self, context: SecurityContext, dataset: str, 
                       classification: DataClassification, action: str, 
                       record_count: Optional[int] = None):
        """Registrar acceso a datos."""
        audit_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': 'data_access',
            'user_id': context.user_id,
            'session_id': context.session_id,
            'dataset': dataset,
            'classification': classification.level.value,
            'action': action,
            'record_count': record_count,
            'pii_accessed': len(classification.pii_fields) > 0
        }
        
        self.audit_logger.info(f"DATA_AUDIT: {json.dumps(audit_entry)}")
    
    def log_security_event(self, event_type: str, context: Optional[SecurityContext], 
                          details: Dict[str, Any]):
        """Registrar evento de seguridad."""
        audit_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': f'security_{event_type}',
            'user_id': context.user_id if context else 'system',
            'session_id': context.session_id if context else None,
            'details': details
        }
        
        self.audit_logger.warning(f"SECURITY_AUDIT: {json.dumps(audit_entry)}")

class DataMasking:
    """Utilidades para enmascaramiento de datos sensibles."""
    
    @staticmethod
    def mask_email(email: str) -> str:
        """Enmascarar dirección de email."""
        if '@' not in email:
            return email
        
        local, domain = email.split('@', 1)
        if len(local) <= 2:
            masked_local = '*' * len(local)
        else:
            masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
        
        return f"{masked_local}@{domain}"
    
    @staticmethod
    def mask_phone(phone: str) -> str:
        """Enmascarar número de teléfono."""
        digits_only = ''.join(filter(str.isdigit, phone))
        if len(digits_only) < 4:
            return '*' * len(phone)
        
        masked = '*' * (len(digits_only) - 4) + digits_only[-4:]
        return phone.replace(digits_only, masked)
    
    @staticmethod
    def mask_credit_card(card_number: str) -> str:
        """Enmascarar número de tarjeta de crédito."""
        digits_only = ''.join(filter(str.isdigit, card_number))
        if len(digits_only) < 4:
            return '*' * len(card_number)
        
        masked = '*' * (len(digits_only) - 4) + digits_only[-4:]
        return card_number.replace(digits_only, masked)
    
    @staticmethod
    def mask_field(value: str, field_type: str) -> str:
        """Enmascarar campo según su tipo."""
        if not value:
            return value
        
        if field_type == 'email':
            return DataMasking.mask_email(value)
        elif field_type == 'phone':
            return DataMasking.mask_phone(value)
        elif field_type == 'credit_card':
            return DataMasking.mask_credit_card(value)
        elif field_type == 'ssn':
            return '*' * (len(value) - 4) + value[-4:] if len(value) >= 4 else '*' * len(value)
        else:
            # Enmascaramiento genérico
            if len(value) <= 2:
                return '*' * len(value)
            return value[0] + '*' * (len(value) - 2) + value[-1]

class SecurityError(Exception):
    """Excepción para errores de seguridad."""
    pass

class AccessDeniedError(SecurityError):
    """Excepción para acceso denegado."""
    pass

class SecurityValidator:
    """Validador de seguridad para operaciones."""
    
    def __init__(self, audit_logger: AuditLogger):
        self.audit_logger = audit_logger
    
    def validate_access(self, context: SecurityContext, resource: str, 
                       required_permission: str) -> bool:
        """
        Validar acceso a recurso.
        
        Args:
            context: Contexto de seguridad
            resource: Recurso solicitado
            required_permission: Permiso requerido
            
        Returns:
            True si tiene acceso
            
        Raises:
            AccessDeniedError: Si no tiene acceso
        """
        # Verificar expiración
        if context.is_expired():
            self.audit_logger.log_security_event(
                'token_expired', context, 
                {'resource': resource, 'permission': required_permission}
            )
            raise AccessDeniedError("Token expirado")
        
        # Verificar permiso
        if not context.has_permission(required_permission):
            self.audit_logger.log_access(
                context, resource, required_permission, False,
                {'reason': 'insufficient_permissions'}
            )
            raise AccessDeniedError(f"Permiso insuficiente: {required_permission}")
        
        # Registrar acceso exitoso
        self.audit_logger.log_access(context, resource, required_permission, True)
        return True
    
    def validate_data_access(self, context: SecurityContext, dataset: str,
                           classification: DataClassification, action: str) -> bool:
        """Validar acceso a datos clasificados."""
        # Verificar nivel de acceso según clasificación
        required_level = AccessLevel.READ
        if classification.level == SecurityLevel.CONFIDENTIAL:
            required_level = AccessLevel.WRITE
        elif classification.level == SecurityLevel.RESTRICTED:
            required_level = AccessLevel.ADMIN
        
        if context.access_level.value < required_level.value:
            self.audit_logger.log_data_access(
                context, dataset, classification, f"{action}_denied"
            )
            raise AccessDeniedError(
                f"Nivel de acceso insuficiente para datos {classification.level.value}"
            )
        
        # Registrar acceso a datos
        self.audit_logger.log_data_access(context, dataset, classification, action)
        return True

def require_permission(permission: str):
    """Decorador para requerir permiso específico."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Buscar contexto de seguridad en argumentos
            context = None
            for arg in args:
                if isinstance(arg, SecurityContext):
                    context = arg
                    break
            
            if 'security_context' in kwargs:
                context = kwargs['security_context']
            
            if context is None:
                raise SecurityError("Contexto de seguridad requerido")
            
            # Validar permiso
            validator = SecurityValidator(AuditLogger())
            validator.validate_access(context, func.__name__, permission)
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def require_data_classification(min_level: SecurityLevel):
    """Decorador para requerir nivel mínimo de clasificación de datos."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Implementar validación de clasificación de datos
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# Instancias globales
_encryption_manager: Optional[EncryptionManager] = None
_token_manager: Optional[TokenManager] = None
_audit_logger: Optional[AuditLogger] = None

def get_encryption_manager() -> EncryptionManager:
    """Obtener instancia global del gestor de encriptación."""
    global _encryption_manager
    if _encryption_manager is None:
        master_key = os.getenv('ENCRYPTION_MASTER_KEY')
        _encryption_manager = EncryptionManager(master_key)
    return _encryption_manager

def get_token_manager() -> TokenManager:
    """Obtener instancia global del gestor de tokens."""
    global _token_manager
    if _token_manager is None:
        secret_key = os.getenv('JWT_SECRET_KEY', secrets.token_urlsafe(32))
        _token_manager = TokenManager(secret_key)
    return _token_manager

def get_audit_logger() -> AuditLogger:
    """Obtener instancia global del logger de auditoría."""
    global _audit_logger
    if _audit_logger is None:
        log_file = os.getenv('AUDIT_LOG_FILE')
        _audit_logger = AuditLogger(log_file)
    return _audit_logger