from django.urls import path
from product_data.views import ProductDataViewSet

urlpatterns = [
    path('upload', ProductDataViewSet.as_view({'post': 'create'})),
]
