from rest_framework.routers import DefaultRouter
from .views import PersonViewSet


router = DefaultRouter()
router.register('', PersonViewSet, basename='cgu_pep_people')

urlpatterns = router.urls
