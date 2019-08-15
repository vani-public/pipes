from pypipes.config import Config, ClientConfig
from pypipes.context import ContextPath


class ConfigPath(ContextPath):
    config_class = Config

    def __call__(self, context_dict):
        result = super(ConfigPath, self).__call__(context_dict)
        # Convert ContextPath result into a Config object
        if result is None or isinstance(result, dict):
            return self.config_class(result or {})
        return result


class ClientConfigPath(ConfigPath):
    config_class = ClientConfig


config = ConfigPath().config
client_config = ClientConfigPath().config
