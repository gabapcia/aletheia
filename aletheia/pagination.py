from django.conf import settings
from django.db.models.query import QuerySet
from django.utils.translation import gettext_lazy as _
from rest_framework.exceptions import ValidationError
from rest_framework.pagination import BasePagination
from rest_framework.response import Response
from rest_framework import status


class PageLimitPagination(BasePagination):
    PAGE_PARAM = getattr(settings.REST_FRAMEWORK.get('PAGINATION', {}), 'PAGE_PARAM', 'page')
    LIMIT_PARAM = getattr(settings.REST_FRAMEWORK.get('PAGINATION', {}), 'LIMIT_PARAM', 'limit')
    MAX_LIMIT_VALUE = getattr(settings.REST_FRAMEWORK.get('PAGINATION', {}), 'MAX_LIMIT_VALUE', 10)
    ORDERING_KEY = getattr(settings.REST_FRAMEWORK.get('PAGINATION', {}), 'ORDERING_KEY', '-created_at')

    def _get_page_value(self, request) -> int:
        try:
            return int(request.query_params.get(PageLimitPagination.PAGE_PARAM, 0))
        except ValueError:
            raise ValidationError(_('Invalid page value'), status.HTTP_422_UNPROCESSABLE_ENTITY)

    def _get_limit_value(self, request) -> int:
        try:
            limit = int(request.query_params.get(PageLimitPagination.LIMIT_PARAM, PageLimitPagination.MAX_LIMIT_VALUE))
        except ValueError:
            raise ValidationError(_('Invalid limit value'), status.HTTP_422_UNPROCESSABLE_ENTITY)

        return limit if limit <= PageLimitPagination.MAX_LIMIT_VALUE else PageLimitPagination.MAX_LIMIT_VALUE

    def paginate_queryset(self, queryset: QuerySet, request, view):
        page = self._get_page_value(request)
        limit = self._get_limit_value(request)

        skip = page * limit
        max_value = skip + limit

        queryset = queryset.order_by(PageLimitPagination.ORDERING_KEY)[skip:max_value]
        return queryset

    def get_paginated_response(self, data):
        return Response(data)
