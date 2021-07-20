from rest_framework.routers import DefaultRouter
from .views import PersonViewSet


router = DefaultRouter()
router.register('', PersonViewSet, basename='cgu_auxilioemergencial_people')

urlpatterns = router.urls
