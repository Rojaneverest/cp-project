from django.urls import path
from . import views

urlpatterns = [
    path('', views.DashboardView.as_view(), name='dashboard'),
    path('api/sales-data/', views.SalesDataAPIView.as_view(), name='sales_data'),
    path('api/real-time-metrics/', views.RealTimeMetricsAPIView.as_view(), name='real_time_metrics'),
] 