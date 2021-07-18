from rest_framework.routers import DefaultRouter
from .views import PaymentViewSet, WithdrawViewSet


router = DefaultRouter()
router.register('payments', PaymentViewSet, basename='cgu_bolsafamilia_payment')
router.register('withdrawals', WithdrawViewSet, basename='cgu_bolsafamilia_withdraw')


urlpatterns = router.urls
