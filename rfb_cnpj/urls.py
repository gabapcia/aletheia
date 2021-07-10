from rest_framework.routers import DefaultRouter
from .views import EstablishmentViewSet


router = DefaultRouter()
router.register(r'companies', EstablishmentViewSet, basename='companies')

urlpatterns = router.urls
