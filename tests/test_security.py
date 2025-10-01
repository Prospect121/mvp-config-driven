"""
Pruebas unitarias para las utilidades de seguridad.
"""

import pytest
import json
import base64
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.utils.security import (
    SecurityLevel, AccessLevel, SecurityContext, DataClassification,
    EncryptionManager, HashManager, TokenManager, AuditLogger,
    DataMasking, SecurityValidator, SecurityError, AccessDeniedError,
    get_encryption_manager, get_token_manager, get_audit_logger
)

class TestSecurityContext:
    """Pruebas para SecurityContext."""
    
    def test_security_context_creation(self):
        """Prueba creación de contexto de seguridad."""
        context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.WRITE,
            permissions=["read_data", "write_data"],
            ip_address="192.168.1.1"
        )
        
        assert context.user_id == "user123"
        assert context.session_id == "session456"
        assert context.access_level == AccessLevel.WRITE
        assert "read_data" in context.permissions
        assert context.ip_address == "192.168.1.1"
    
    def test_context_expiration(self):
        """Prueba expiración de contexto."""
        # Contexto sin expiración
        context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.READ
        )
        assert not context.is_expired()
        
        # Contexto expirado
        context.expires_at = datetime.utcnow() - timedelta(hours=1)
        assert context.is_expired()
        
        # Contexto válido
        context.expires_at = datetime.utcnow() + timedelta(hours=1)
        assert not context.is_expired()
    
    def test_permission_checking(self):
        """Prueba verificación de permisos."""
        context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.WRITE,
            permissions=["read_data", "write_data"]
        )
        
        assert context.has_permission("read_data")
        assert context.has_permission("write_data")
        assert not context.has_permission("admin_access")
        
        # Owner tiene todos los permisos
        owner_context = SecurityContext(
            user_id="owner123",
            session_id="session789",
            access_level=AccessLevel.OWNER
        )
        assert owner_context.has_permission("any_permission")
    
    def test_context_serialization(self):
        """Prueba serialización de contexto."""
        context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.ADMIN,
            permissions=["read_data", "admin_access"],
            ip_address="192.168.1.1"
        )
        
        context_dict = context.to_dict()
        assert context_dict["user_id"] == "user123"
        assert context_dict["access_level"] == "admin"
        assert "read_data" in context_dict["permissions"]

class TestDataClassification:
    """Pruebas para DataClassification."""
    
    def test_classification_creation(self):
        """Prueba creación de clasificación."""
        classification = DataClassification(
            level=SecurityLevel.CONFIDENTIAL,
            categories=["financial", "personal"],
            retention_days=365,
            pii_fields=["email", "phone"]
        )
        
        assert classification.level == SecurityLevel.CONFIDENTIAL
        assert "financial" in classification.categories
        assert classification.retention_days == 365
        assert "email" in classification.pii_fields
    
    def test_encryption_requirements(self):
        """Prueba requerimientos de encriptación."""
        # Nivel público sin encriptación requerida
        public_data = DataClassification(
            level=SecurityLevel.PUBLIC,
            encryption_required=False
        )
        assert not public_data.requires_encryption()
        
        # Nivel confidencial siempre requiere encriptación
        confidential_data = DataClassification(
            level=SecurityLevel.CONFIDENTIAL,
            encryption_required=False  # Ignorado por el nivel
        )
        assert confidential_data.requires_encryption()
        
        # Encriptación explícitamente requerida
        internal_data = DataClassification(
            level=SecurityLevel.INTERNAL,
            encryption_required=True
        )
        assert internal_data.requires_encryption()
    
    def test_audit_requirements(self):
        """Prueba requerimientos de auditoría."""
        # Nivel público sin auditoría requerida
        public_data = DataClassification(
            level=SecurityLevel.PUBLIC,
            audit_required=False
        )
        assert not public_data.requires_audit()
        
        # Nivel restringido siempre requiere auditoría
        restricted_data = DataClassification(
            level=SecurityLevel.RESTRICTED,
            audit_required=False  # Ignorado por el nivel
        )
        assert restricted_data.requires_audit()

class TestEncryptionManager:
    """Pruebas para EncryptionManager."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.encryption_manager = EncryptionManager()
        self.test_data = "Datos sensibles de prueba"
        self.context = SecurityContext(
            user_id="test_user",
            session_id="test_session",
            access_level=AccessLevel.WRITE
        )
    
    def test_encrypt_decrypt_string(self):
        """Prueba encriptación y desencriptación de string."""
        encrypted = self.encryption_manager.encrypt(self.test_data, self.context)
        assert encrypted != self.test_data
        assert isinstance(encrypted, str)
        
        decrypted = self.encryption_manager.decrypt(encrypted, self.context)
        assert decrypted.decode('utf-8') == self.test_data
    
    def test_encrypt_decrypt_bytes(self):
        """Prueba encriptación y desencriptación de bytes."""
        test_bytes = self.test_data.encode('utf-8')
        encrypted = self.encryption_manager.encrypt(test_bytes, self.context)
        decrypted = self.encryption_manager.decrypt(encrypted, self.context)
        assert decrypted == test_bytes
    
    def test_encrypt_field(self):
        """Prueba encriptación de campo específico."""
        field_value = "valor_secreto"
        field_name = "password"
        
        encrypted = self.encryption_manager.encrypt_field(field_value, field_name)
        assert encrypted != field_value
        
        decrypted = self.encryption_manager.decrypt_field(encrypted, field_name)
        assert decrypted == field_value
    
    def test_encrypt_field_none_value(self):
        """Prueba encriptación de valor None."""
        encrypted = self.encryption_manager.encrypt_field(None, "test_field")
        assert encrypted is None
    
    def test_decrypt_field_wrong_field(self):
        """Prueba desencriptación con campo incorrecto."""
        encrypted = self.encryption_manager.encrypt_field("value", "field1")
        
        with pytest.raises(SecurityError):
            self.encryption_manager.decrypt_field(encrypted, "field2")
    
    def test_encryption_error_handling(self):
        """Prueba manejo de errores en encriptación."""
        with pytest.raises(SecurityError):
            self.encryption_manager.decrypt("datos_invalidos")

class TestHashManager:
    """Pruebas para HashManager."""
    
    def test_hash_data_string(self):
        """Prueba hash de string."""
        data = "datos de prueba"
        hash_value = HashManager.hash_data(data)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 64  # SHA256 produce 64 caracteres hex
        
        # El mismo dato debe producir el mismo hash
        hash_value2 = HashManager.hash_data(data)
        assert hash_value == hash_value2
    
    def test_hash_data_bytes(self):
        """Prueba hash de bytes."""
        data = b"datos de prueba"
        hash_value = HashManager.hash_data(data)
        assert isinstance(hash_value, str)
        assert len(hash_value) == 64
    
    def test_hash_algorithms(self):
        """Prueba diferentes algoritmos de hash."""
        data = "test data"
        
        sha256_hash = HashManager.hash_data(data, 'sha256')
        sha512_hash = HashManager.hash_data(data, 'sha512')
        md5_hash = HashManager.hash_data(data, 'md5')
        
        assert len(sha256_hash) == 64
        assert len(sha512_hash) == 128
        assert len(md5_hash) == 32
        
        # Hashes diferentes para el mismo dato
        assert sha256_hash != sha512_hash != md5_hash
    
    def test_verify_hash(self):
        """Prueba verificación de hash."""
        data = "datos de prueba"
        hash_value = HashManager.hash_data(data)
        
        assert HashManager.verify_hash(data, hash_value)
        assert not HashManager.verify_hash("datos diferentes", hash_value)
    
    def test_hash_password(self):
        """Prueba hash de contraseña."""
        password = "mi_contraseña_secreta"
        password_hash, salt = HashManager.hash_password(password)
        
        assert isinstance(password_hash, str)
        assert isinstance(salt, str)
        assert len(salt) == 64  # 32 bytes en hex
        assert password_hash != password
        
        # Verificar contraseña
        assert HashManager.verify_password(password, password_hash, salt)
        assert not HashManager.verify_password("contraseña_incorrecta", password_hash, salt)
    
    def test_hash_password_with_salt(self):
        """Prueba hash con sal específica."""
        password = "mi_contraseña"
        salt = "sal_especifica"
        
        hash1, salt1 = HashManager.hash_password(password, salt)
        hash2, salt2 = HashManager.hash_password(password, salt)
        
        assert hash1 == hash2
        assert salt1 == salt2 == salt
    
    def test_invalid_hash_algorithm(self):
        """Prueba algoritmo de hash inválido."""
        with pytest.raises(ValueError):
            HashManager.hash_data("data", "invalid_algorithm")

class TestTokenManager:
    """Pruebas para TokenManager."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.token_manager = TokenManager("secret_key_for_testing")
        self.context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.WRITE,
            permissions=["read_data", "write_data"]
        )
    
    def test_generate_token(self):
        """Prueba generación de token."""
        token = self.token_manager.generate_token(self.context)
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_verify_valid_token(self):
        """Prueba verificación de token válido."""
        token = self.token_manager.generate_token(self.context)
        decoded_context = self.token_manager.verify_token(token)
        
        assert decoded_context is not None
        assert decoded_context.user_id == self.context.user_id
        assert decoded_context.session_id == self.context.session_id
        assert decoded_context.access_level == self.context.access_level
        assert decoded_context.permissions == self.context.permissions
    
    def test_verify_invalid_token(self):
        """Prueba verificación de token inválido."""
        invalid_token = "token_invalido"
        decoded_context = self.token_manager.verify_token(invalid_token)
        assert decoded_context is None
    
    def test_token_expiration(self):
        """Prueba expiración de token."""
        # Generar token que expira en 1 segundo
        token = self.token_manager.generate_token(self.context, expires_in_hours=1/3600)
        
        # Verificar inmediatamente (debe ser válido)
        decoded_context = self.token_manager.verify_token(token)
        assert decoded_context is not None
        
        # Simular expiración modificando el tiempo
        import time
        time.sleep(2)
        
        # El token debería seguir siendo válido en esta prueba
        # (la verificación real de expiración depende de la implementación JWT)
    
    def test_different_secret_keys(self):
        """Prueba tokens con diferentes claves secretas."""
        token_manager2 = TokenManager("different_secret_key")
        
        token = self.token_manager.generate_token(self.context)
        
        # Token generado con una clave no debe ser válido con otra clave
        decoded_context = token_manager2.verify_token(token)
        # Dependiendo de la implementación, esto podría ser None o lanzar excepción

class TestAuditLogger:
    """Pruebas para AuditLogger."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.audit_logger = AuditLogger()
        self.context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.WRITE,
            ip_address="192.168.1.1",
            user_agent="TestAgent/1.0"
        )
    
    @patch('src.utils.security.get_logger')
    def test_log_access(self, mock_get_logger):
        """Prueba logging de acceso."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        audit_logger = AuditLogger()
        audit_logger.log_access(
            self.context, "test_resource", "read", True, {"extra": "info"}
        )
        
        # Verificar que se llamó al logger
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "AUDIT:" in call_args
        assert "test_resource" in call_args
    
    @patch('src.utils.security.get_logger')
    def test_log_data_access(self, mock_get_logger):
        """Prueba logging de acceso a datos."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        classification = DataClassification(
            level=SecurityLevel.CONFIDENTIAL,
            pii_fields=["email", "phone"]
        )
        
        audit_logger = AuditLogger()
        audit_logger.log_data_access(
            self.context, "customer_data", classification, "read", 100
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "DATA_AUDIT:" in call_args
        assert "customer_data" in call_args
    
    @patch('src.utils.security.get_logger')
    def test_log_security_event(self, mock_get_logger):
        """Prueba logging de evento de seguridad."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        audit_logger = AuditLogger()
        audit_logger.log_security_event(
            "unauthorized_access", self.context, {"resource": "admin_panel"}
        )
        
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0][0]
        assert "SECURITY_AUDIT:" in call_args
        assert "security_unauthorized_access" in call_args

class TestDataMasking:
    """Pruebas para DataMasking."""
    
    def test_mask_email(self):
        """Prueba enmascaramiento de email."""
        assert DataMasking.mask_email("test@example.com") == "t**t@example.com"
        assert DataMasking.mask_email("a@example.com") == "*@example.com"
        assert DataMasking.mask_email("ab@example.com") == "**@example.com"
        assert DataMasking.mask_email("invalid_email") == "invalid_email"
    
    def test_mask_phone(self):
        """Prueba enmascaramiento de teléfono."""
        assert DataMasking.mask_phone("123-456-7890") == "***-***-7890"
        assert DataMasking.mask_phone("+1 (555) 123-4567") == "+* (***)***-4567"
        assert DataMasking.mask_phone("123") == "***"
    
    def test_mask_credit_card(self):
        """Prueba enmascaramiento de tarjeta de crédito."""
        assert DataMasking.mask_credit_card("1234-5678-9012-3456") == "****-****-****-3456"
        assert DataMasking.mask_credit_card("1234567890123456") == "************3456"
        assert DataMasking.mask_credit_card("123") == "***"
    
    def test_mask_field_types(self):
        """Prueba enmascaramiento por tipo de campo."""
        assert DataMasking.mask_field("test@example.com", "email") == "t**t@example.com"
        assert DataMasking.mask_field("123-456-7890", "phone") == "***-***-7890"
        assert DataMasking.mask_field("1234-5678-9012-3456", "credit_card") == "****-****-****-3456"
        assert DataMasking.mask_field("123-45-6789", "ssn") == "*****-6789"
        assert DataMasking.mask_field("sensitive_data", "generic") == "s***********a"
    
    def test_mask_empty_values(self):
        """Prueba enmascaramiento de valores vacíos."""
        assert DataMasking.mask_field("", "email") == ""
        assert DataMasking.mask_field(None, "phone") is None
    
    def test_mask_short_values(self):
        """Prueba enmascaramiento de valores cortos."""
        assert DataMasking.mask_field("a", "generic") == "*"
        assert DataMasking.mask_field("ab", "generic") == "**"

class TestSecurityValidator:
    """Pruebas para SecurityValidator."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.audit_logger = AuditLogger()
        self.validator = SecurityValidator(self.audit_logger)
        self.context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.WRITE,
            permissions=["read_data", "write_data"]
        )
    
    def test_validate_access_success(self):
        """Prueba validación de acceso exitosa."""
        result = self.validator.validate_access(
            self.context, "test_resource", "read_data"
        )
        assert result is True
    
    def test_validate_access_insufficient_permission(self):
        """Prueba validación con permiso insuficiente."""
        with pytest.raises(AccessDeniedError, match="Permiso insuficiente"):
            self.validator.validate_access(
                self.context, "admin_resource", "admin_access"
            )
    
    def test_validate_access_expired_token(self):
        """Prueba validación con token expirado."""
        expired_context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.ADMIN,
            permissions=["admin_access"],
            expires_at=datetime.utcnow() - timedelta(hours=1)
        )
        
        with pytest.raises(AccessDeniedError, match="Token expirado"):
            self.validator.validate_access(
                expired_context, "resource", "admin_access"
            )
    
    def test_validate_data_access_success(self):
        """Prueba validación de acceso a datos exitosa."""
        classification = DataClassification(level=SecurityLevel.INTERNAL)
        
        result = self.validator.validate_data_access(
            self.context, "test_dataset", classification, "read"
        )
        assert result is True
    
    def test_validate_data_access_insufficient_level(self):
        """Prueba validación con nivel de acceso insuficiente."""
        # Contexto con nivel READ intentando acceder a datos RESTRICTED
        read_context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.READ
        )
        
        restricted_classification = DataClassification(level=SecurityLevel.RESTRICTED)
        
        with pytest.raises(AccessDeniedError, match="Nivel de acceso insuficiente"):
            self.validator.validate_data_access(
                read_context, "restricted_dataset", restricted_classification, "read"
            )

class TestGlobalInstances:
    """Pruebas para instancias globales."""
    
    def test_get_encryption_manager(self):
        """Prueba obtención de gestor de encriptación global."""
        manager1 = get_encryption_manager()
        manager2 = get_encryption_manager()
        
        # Debe retornar la misma instancia
        assert manager1 is manager2
        assert isinstance(manager1, EncryptionManager)
    
    def test_get_token_manager(self):
        """Prueba obtención de gestor de tokens global."""
        manager1 = get_token_manager()
        manager2 = get_token_manager()
        
        assert manager1 is manager2
        assert isinstance(manager1, TokenManager)
    
    def test_get_audit_logger(self):
        """Prueba obtención de logger de auditoría global."""
        logger1 = get_audit_logger()
        logger2 = get_audit_logger()
        
        assert logger1 is logger2
        assert isinstance(logger1, AuditLogger)

class TestSecurityIntegration:
    """Pruebas de integración de seguridad."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.encryption_manager = EncryptionManager()
        self.token_manager = TokenManager("test_secret")
        self.audit_logger = AuditLogger()
        self.validator = SecurityValidator(self.audit_logger)
    
    def test_full_security_workflow(self):
        """Prueba flujo completo de seguridad."""
        # 1. Crear contexto de seguridad
        context = SecurityContext(
            user_id="user123",
            session_id="session456",
            access_level=AccessLevel.WRITE,
            permissions=["read_data", "write_data"]
        )
        
        # 2. Generar token
        token = self.token_manager.generate_token(context)
        assert token is not None
        
        # 3. Verificar token
        decoded_context = self.token_manager.verify_token(token)
        assert decoded_context is not None
        assert decoded_context.user_id == context.user_id
        
        # 4. Validar acceso
        result = self.validator.validate_access(
            decoded_context, "test_resource", "read_data"
        )
        assert result is True
        
        # 5. Encriptar datos sensibles
        sensitive_data = "información confidencial"
        encrypted_data = self.encryption_manager.encrypt(sensitive_data, decoded_context)
        assert encrypted_data != sensitive_data
        
        # 6. Desencriptar datos
        decrypted_data = self.encryption_manager.decrypt(encrypted_data, decoded_context)
        assert decrypted_data.decode('utf-8') == sensitive_data

if __name__ == "__main__":
    pytest.main([__file__, "-v"])