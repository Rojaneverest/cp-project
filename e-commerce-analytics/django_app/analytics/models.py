from django.db import models

# Note: These are just proxy models for the tables created by the Spark job
# They don't create tables, but allow Django to interact with existing tables

class OrderSummary(models.Model):
    """
    Model representing the order_summary table in the data warehouse.
    """
    order_id = models.CharField(max_length=255, primary_key=True)
    user_id = models.CharField(max_length=255)
    order_date = models.DateTimeField()
    status = models.CharField(max_length=50)
    total_amount = models.DecimalField(max_digits=10, decimal_places=2)
    year = models.IntegerField()
    month = models.IntegerField()
    day = models.IntegerField()
    hour = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'order_summary'


class OrderItem(models.Model):
    """
    Model representing the order_items table in the data warehouse.
    """
    id = models.AutoField(primary_key=True)
    order_id = models.CharField(max_length=255)
    order_date = models.DateTimeField()
    year = models.IntegerField()
    month = models.IntegerField()
    day = models.IntegerField()
    product_id = models.CharField(max_length=255)
    quantity = models.IntegerField()
    price = models.DecimalField(max_digits=10, decimal_places=2)
    item_total = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        managed = False
        db_table = 'order_items' 