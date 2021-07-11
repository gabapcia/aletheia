from django.contrib import admin
from django.urls import path, include
from drf_spectacular.views import SpectacularRedocView, SpectacularAPIView


urlpatterns = [
    path('admin/', admin.site.urls),
    path('schema/', SpectacularAPIView.as_view(), name='schema'),
    path('redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),

    path('api/', include([
        path('v1/', include([
            path('rfb/', include([
                path('cnpj/', include('rfb_cnpj.urls'), name='RFB CNPJ'),
            ])),
            path('cgu/', include([
                path('pep/', include('cgu_pep.urls'), name='CGU PEP'),
            ])),
        ]))
    ])),
]
