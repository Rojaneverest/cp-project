"""
ASGI config for ecommerce_analytics project.
"""

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
import analytics.routing

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ecommerce_analytics.settings')

application = ProtocolTypeRouter({
    'http': get_asgi_application(),
    'websocket': AuthMiddlewareStack(
        URLRouter(
            analytics.routing.websocket_urlpatterns
        )
    ),
}) 