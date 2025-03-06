import pika
import json
import logging
import time
import re
import random
from typing import Dict, List, Any, Optional
import threading
import math

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CopperTopAgent:
    def __init__(self):
        self.queue_name = 'copper_top'
        self.connection = None
        self.channel = None
        self.connected = False
        self.connect_with_retry()

    def connect_with_retry(self, max_retries=10, retry_delay=5):
        """Connect to RabbitMQ with retry mechanism"""
        retry_count = 0
        while retry_count < max_retries:
            try:
                logger.info(f"Attempting to connect to RabbitMQ (attempt {retry_count+1})")
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host='rabbitmq',
                        connection_attempts=5,
                        retry_delay=5
                    )
                )
                self.channel = self.connection.channel()
                
                # Declare the queue
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                
                # Set QoS to process one message at a time
                self.channel.basic_qos(prefetch_count=1)
                
                logger.info("Successfully connected to RabbitMQ")
                self.connected = True
                return True
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to RabbitMQ.")
                    return False

    def start_consuming(self):
        """Start consuming messages from the queue"""
        if not self.connected:
            logger.error("Not connected to RabbitMQ. Cannot start consuming.")
            return False

        try:
            logger.info(f"Starting to consume messages from {self.queue_name} queue")
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.process_message,
                auto_ack=False
            )
            self.channel.start_consuming()
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            self.connected = False
            return False

    def process_message(self, ch, method, properties, body):
        """Process a message from the queue"""
        try:
            logger.info(f"Received message from {self.queue_name} queue")
            message = json.loads(body)
            job_id = message.get('job_id')
            
            if not job_id:
                logger.error("Message missing job_id")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            logger.info(f"Processing job {job_id}")
            
            # Extract required information from the message
            requirements = message.get('requirements', [])
            category = message.get('category', 'unknown')
            callback_queue = message.get('callback_queue')
            
            if not callback_queue:
                logger.error(f"No callback queue specified for job {job_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
                
            # Process the requirements and generate a Gerber file
            try:
                gerber_content, file_name = self.generate_gerber_file(job_id, requirements, category)
                
                # Send success response
                self.send_callback(
                    callback_queue=callback_queue,
                    job_id=job_id,
                    agent_name=self.queue_name,
                    result={
                        "success": True,
                        "file_content": gerber_content,
                        "file_name": file_name
                    }
                )
                logger.info(f"Successfully processed job {job_id}")
            except Exception as e:
                error_msg = f"Error processing requirements for job {job_id}: {str(e)}"
                logger.error(error_msg)
                
                # Send error response
                self.send_callback(
                    callback_queue=callback_queue,
                    job_id=job_id,
                    agent_name=self.queue_name,
                    result={
                        "success": False,
                        "error": error_msg
                    }
                )
            
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {e}")
            # Acknowledge malformed messages to remove them from the queue
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}")
            # Acknowledge the message to avoid getting stuck
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_callback(self, callback_queue: str, job_id: str, agent_name: str, result: Dict[str, Any]):
        """Send a callback message to the boss agent"""
        try:
            message = {
                "job_id": job_id,
                "agent_name": agent_name,
                "result": result
            }
            
            self.channel.basic_publish(
                exchange='',
                routing_key=callback_queue,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            logger.info(f"Sent callback for job {job_id} to {callback_queue}")
        except Exception as e:
            logger.error(f"Error sending callback: {e}")

    def generate_gerber_file(self, job_id: str, requirements: List[str], category: str) -> tuple:
        """
        Generate a Gerber file based on the requirements
        Returns: (gerber_content, file_name)
        """
        logger.info(f"Generating copper top layer Gerber for job {job_id}")
        
        # Extract relevant parameters from requirements
        board_width = 100.0  # Default values
        board_height = 100.0
        trace_width = 0.2
        clearance = 0.2
        
        # Process requirements to extract parameters
        for req in requirements:
            # Skip if requirement is None or not a string
            if not req or not isinstance(req, str):
                continue
                
            # Look for board dimensions
            dim_match = re.search(r'(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)', req)
            if dim_match:
                board_width = float(dim_match.group(1))
                board_height = float(dim_match.group(2))
                
            # Look for trace width
            trace_match = re.search(r'trace.*?(\d+\.?\d*)', req.lower())
            if trace_match:
                trace_width = float(trace_match.group(1))
                
            # Look for clearance
            clearance_match = re.search(r'clearance.*?(\d+\.?\d*)', req.lower())
            if clearance_match:
                clearance = float(clearance_match.group(1))

        # Generate a simple Gerber file for the copper top layer
        gerber_content = self.create_gerber_copper_top(
            board_width, board_height, trace_width, clearance
        )
        
        # Return the Gerber content and filename
        file_name = f"copper_top.gbr"
        return gerber_content, file_name

    def create_gerber_copper_top(self, width: float, height: float, 
                                trace_width: float, clearance: float) -> str:
        """Create a Gerber file for a copper top layer with some traces"""
        # Header
        gerber = []
        gerber.append("%FSLAX36Y36*%")  # Format specification
        gerber.append("%MOMM*%")  # Units in mm
        gerber.append("%LPD*%")  # Layer polarity - dark
        
        # Aperture definitions
        gerber.append("%ADD10C,{:.3f}*%".format(trace_width))  # Circular aperture for traces
        gerber.append("%ADD11C,1.000*%")  # Circular aperture for pads
        gerber.append("%ADD12R,1.800X1.800*%")  # Rectangular aperture for pads
        
        # Draw outline
        gerber.append("G01*")  # Linear interpolation
        gerber.append("D10*")  # Select aperture 10
        gerber.append("X0Y0D02*")  # Move to origin
        gerber.append("X{}Y0D01*".format(int(width * 1000000)))  # Draw to right
        gerber.append("X{}Y{}D01*".format(int(width * 1000000), int(height * 1000000)))  # Draw to top-right
        gerber.append("X0Y{}D01*".format(int(height * 1000000)))  # Draw to top-left
        gerber.append("X0Y0D01*")  # Draw to origin
        
        # Generate some random traces
        num_traces = random.randint(10, 30)
        for i in range(num_traces):
            x1 = random.uniform(clearance, width - clearance)
            y1 = random.uniform(clearance, height - clearance)
            x2 = random.uniform(clearance, width - clearance)
            y2 = random.uniform(clearance, height - clearance)
            
            gerber.append("X{}Y{}D02*".format(int(x1 * 1000000), int(y1 * 1000000)))  # Move to start point
            gerber.append("X{}Y{}D01*".format(int(x2 * 1000000), int(y2 * 1000000)))  # Draw to end point
        
        # Add some pads (component mounting points)
        num_pads = random.randint(15, 40)
        for i in range(num_pads):
            x = random.uniform(clearance, width - clearance)
            y = random.uniform(clearance, height - clearance)
            aperture = "D11" if random.random() > 0.5 else "D12"  # Randomly select pad shape
            
            gerber.append("{}*".format(aperture))  # Select aperture
            gerber.append("X{}Y{}D03*".format(int(x * 1000000), int(y * 1000000)))  # Flash pad
        
        # End of file
        gerber.append("M02*")
        
        return "\n".join(gerber)

    def reconnect_if_needed(self):
        """Check connection status and reconnect if necessary"""
        if not self.connected or (self.connection and not self.connection.is_open):
            logger.info("Connection lost, attempting to reconnect...")
            self.connect_with_retry()
            return self.connected
        return True

    def run(self):
        """Main run loop with reconnection support"""
        while True:
            try:
                if self.reconnect_if_needed():
                    self.start_consuming()
                else:
                    logger.error("Failed to reconnect, waiting before retry...")
                    time.sleep(10)
            except KeyboardInterrupt:
                logger.info("Interrupted by user, shutting down...")
                if self.connection and self.connection.is_open:
                    self.connection.close()
                break
            except Exception as e:
                logger.error(f"Unexpected error in run loop: {e}")
                time.sleep(5)

def health_check_thread(agent):
    """Thread to periodically check agent health"""
    while True:
        try:
            if not agent.reconnect_if_needed():
                logger.warning("Health check: Agent disconnected, reconnection handled in main thread")
        except Exception as e:
            logger.error(f"Error in health check: {e}")
        time.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    agent = CopperTopAgent()
    
    # Start health check in a separate thread
    health_thread = threading.Thread(target=health_check_thread, args=(agent,), daemon=True)
    health_thread.start()
    
    # Run the agent
    agent.run()