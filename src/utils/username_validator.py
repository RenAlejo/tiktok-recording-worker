import re
from typing import Tuple
from utils.logger_manager import logger
from utils.language_messages import LanguageMessages
from config.env_config import config

class UsernameValidator:
    """
    Validador de usernames de TikTok para evitar requests innecesarios de API
    """
    
    # Reglas de TikTok para usernames:
    # - Solo letras, números, puntos, guiones bajos y guiones
    # - Entre 2 y 24 caracteres (configurables)
    # - Puntos válidos en cualquier posición (.user, user., us.er)
    # - No puede ser solo puntos (.., ...)
    TIKTOK_USERNAME_PATTERN = re.compile(rf'^[a-zA-Z0-9._-]{{{config.username_min_length},{config.username_max_length}}}$')
    
    @classmethod
    def is_valid_username(cls, username: str) -> Tuple[bool, str]:
        """
        Valida si un username cumple con las reglas de TikTok
        
        Returns:
            Tuple[bool, str]: (es_válido, tipo_de_error)
        """
        if not username:
            return False, "empty"
        
        # Remover @ si está presente
        clean_username = username.lstrip('@')
        
        # Verificar longitud
        if len(clean_username) < config.username_min_length:
            return False, "too_short"
        
        if len(clean_username) > config.username_max_length:
            return False, "too_long"
        
        # Verificar caracteres permitidos
        if not cls.TIKTOK_USERNAME_PATTERN.match(clean_username):
            return False, "invalid_characters"
        
        # Verificar que no sea solo puntos y otros caracteres similares
        if re.match(r'^[._-]+$', clean_username):
            return False, "only_special_chars"
        
        # Verificar patrones comunes de usernames inválidos
        invalid_patterns = [
            r'^\d+$',  # Solo números
            r'.*[\s]+.*',  # Espacios
            r'.*[<>\"\']+.*',  # Caracteres HTML/especiales
            r'.*[@#$%^&*()]+.*',  # Símbolos especiales
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, clean_username):
                return False, "invalid_format"
        
        return True, ""
    
    @classmethod
    def clean_username(cls, username: str) -> str:
        """
        Limpia el username removiendo caracteres no permitidos
        """
        if not username:
            return ""
        
        # Remover @ inicial si está presente
        clean = username.lstrip('@')
        
        # Remover espacios y caracteres especiales no permitidos (mantener letras, números, puntos, guiones bajos y guiones)
        clean = re.sub(r'[^\w.\-]', '', clean)
        
        return clean[:config.username_max_length]  # Truncar al máximo configurado
    
    @classmethod
    def validate_and_clean(cls, username: str) -> Tuple[bool, str, str]:
        """
        Valida y limpia el username
        
        Returns:
            Tuple[bool, str, str]: (es_válido, username_limpio, tipo_de_error)
        """
        if not username:
            return False, "", "empty"
        
        clean_username = cls.clean_username(username)
        is_valid, error_type = cls.is_valid_username(clean_username)
        
        return is_valid, clean_username, error_type
    
    @classmethod
    def get_error_message(cls, language: str = "en") -> str:
        """
        Obtiene el mensaje de error de validación según el idioma
        
        Args:
            language: Código de idioma ("en", "es", "pt")
            
        Returns:
            str: Mensaje de error localizado
        """
        if language == "es":
            return LanguageMessages.USERNAME_INVALID_ES.value
        elif language == "pt":
            return LanguageMessages.USERNAME_INVALID_PT.value
        else:
            return LanguageMessages.USERNAME_INVALID_EN.value