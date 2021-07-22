from rest_framework.routers import DefaultRouter
from .views import WarrantyViewSet


router = DefaultRouter()
router.register('', WarrantyViewSet, basename='cgu_garantiasafra_warranty')

urlpatterns = router.urls
