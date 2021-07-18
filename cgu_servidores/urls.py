from rest_framework.routers import DefaultRouter
from .views import EmployeeViewSet, RetiredViewSet, PensionerViewSet


router = DefaultRouter()
router.register('employees', EmployeeViewSet, basename='cgu_servidores_employees')
router.register('retired', RetiredViewSet, basename='cgu_servidores_retired')
router.register('pensioners', PensionerViewSet, basename='cgu_servidores_pensioners')

urlpatterns = router.urls
