from rest_framework.routers import DefaultRouter
from .views import PaymentViewSet, WithdrawViewSet


router = DefaultRouter()
router.register('payments', PaymentViewSet, basename='bolsafamilia-payment')
router.register('withdrawals', WithdrawViewSet, basename='bolsafamilia-withdraw')


urlpatterns = router.urls
