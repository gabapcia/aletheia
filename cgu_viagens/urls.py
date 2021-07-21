from rest_framework.routers import DefaultRouter
from .views import TripViewSet


router = DefaultRouter()
router.register('', TripViewSet, basename='cgu_viagens_trips')

urlpatterns = router.urls
