from django.shortcuts import render
from django.db.models import Sum, Count, Avg
from django.http import JsonResponse
from django.views.generic import TemplateView
from rest_framework.views import APIView
from rest_framework.response import Response
from .models import OrderSummary, OrderItem
import redis
import json


class DashboardView(TemplateView):
    """
    Main dashboard view for the analytics platform.
    """
    template_name = 'analytics/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Add any context data needed for the template
        # (actual data will be loaded via API and WebSockets)
        return context


class SalesDataAPIView(APIView):
    """
    API endpoint to provide sales data for charts.
    """
    def get(self, request):
        # Get sales by day from the order_summary table
        sales_by_day = (
            OrderSummary.objects
            .filter(status='completed')
            .values('year', 'month', 'day')
            .annotate(
                total_sales=Sum('total_amount'),
                order_count=Count('order_id'),
                avg_order_value=Avg('total_amount')
            )
            .order_by('year', 'month', 'day')
        )
        
        # Get top products from the order_items table
        top_products = (
            OrderItem.objects
            .values('product_id')
            .annotate(
                total_quantity=Sum('quantity'),
                total_revenue=Sum('item_total')
            )
            .order_by('-total_revenue')[:10]
        )
        
        return Response({
            'sales_by_day': list(sales_by_day),
            'top_products': list(top_products)
        })


class RealTimeMetricsAPIView(APIView):
    """
    API endpoint to provide real-time metrics from Redis.
    """
    def get(self, request):
        try:
            r = redis.Redis(host='redis', port=6379, db=0)
            
            # Get event counts
            event_counts = r.hgetall('event_counts')
            if event_counts:
                event_counts = {k.decode(): int(v) for k, v in event_counts.items()}
            else:
                event_counts = {}
            
            # Get page views
            page_views = r.hgetall('page_views')
            if page_views:
                page_views = {k.decode(): int(v) for k, v in page_views.items()}
            else:
                page_views = {}
            
            # Get active users count
            active_users_count = r.zcard('active_users')
            
            # Get total revenue
            total_revenue_cents = r.get('total_revenue_cents')
            total_revenue = float(total_revenue_cents) / 100 if total_revenue_cents else 0
            
            return Response({
                'event_counts': event_counts,
                'page_views': page_views,
                'active_users_count': active_users_count,
                'total_revenue': total_revenue
            })
            
        except Exception as e:
            return Response({'error': str(e)}, status=500) 