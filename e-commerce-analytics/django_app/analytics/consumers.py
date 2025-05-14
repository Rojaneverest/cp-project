import json
import asyncio
import redis.asyncio as aioredis
from channels.generic.websocket import AsyncWebsocketConsumer


class DashboardConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for real-time dashboard updates.
    """
    async def connect(self):
        # Add the channel to a group
        await self.channel_layer.group_add('dashboard', self.channel_name)
        await self.accept()
        
        # Start sending regular updates
        self.send_updates_task = asyncio.create_task(self.send_regular_updates())
    
    async def disconnect(self, close_code):
        # Remove the channel from the group
        await self.channel_layer.group_discard('dashboard', self.channel_name)
        
        # Cancel the update task
        if hasattr(self, 'send_updates_task'):
            self.send_updates_task.cancel()
            try:
                await self.send_updates_task
            except asyncio.CancelledError:
                pass
    
    async def send_regular_updates(self):
        """
        Send regular updates with real-time metrics.
        """
        try:
            # Connect to Redis
            r = await aioredis.from_url('redis://redis:6379/0')
            
            while True:
                # Fetch metrics from Redis
                event_counts = await r.hgetall('event_counts')
                if event_counts:
                    event_counts = {k.decode(): int(v) for k, v in event_counts.items()}
                else:
                    event_counts = {}
                
                page_views = await r.hgetall('page_views')
                if page_views:
                    page_views = {k.decode(): int(v) for k, v in page_views.items()}
                else:
                    page_views = {}
                
                active_users_count = await r.zcard('active_users')
                
                total_revenue_cents = await r.get('total_revenue_cents')
                total_revenue = float(total_revenue_cents) / 100 if total_revenue_cents else 0
                
                # Prepare the metrics payload
                metrics = {
                    'event_counts': event_counts,
                    'page_views': page_views,
                    'active_users_count': active_users_count,
                    'total_revenue': total_revenue,
                    'timestamp': asyncio.get_event_loop().time()
                }
                
                # Send to the group
                await self.channel_layer.group_send(
                    'dashboard',
                    {
                        'type': 'send_metrics_update',
                        'metrics': metrics
                    }
                )
                
                # Wait before sending the next update
                await asyncio.sleep(3)  # Send updates every 3 seconds
                
        except asyncio.CancelledError:
            # Task was cancelled, clean up
            await r.close()
            raise
        except Exception as e:
            print(f"Error in send_regular_updates: {e}")
            await asyncio.sleep(5)  # Wait before trying again
    
    async def send_metrics_update(self, event):
        """
        Send metrics update to the WebSocket.
        """
        metrics = event['metrics']
        await self.send(text_data=json.dumps(metrics)) 