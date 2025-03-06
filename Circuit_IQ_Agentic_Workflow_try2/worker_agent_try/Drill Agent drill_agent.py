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

class DrillAgent:
    def __init__(self):
        self.queue_name = 'drill'
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
                
            # Process the requirements and generate a drill file
            try:
                # Generate Excellon drill file and Gerber drill map
                excellon_content, excellon_filename = self.generate_excellon_file(job_id, requirements, category)
                gerber_map, gerber_map_filename = self.generate_drill_map(job_id, requirements, category)
                
                # Send success response for Excellon file
                self.send_callback(
                    callback_queue=callback_queue,
                    job_id=job_id,
                    agent_name=f"{self.queue_name}_excellon",
                    result={
                        "success": True,
                        "file_content": excellon_content,
                        "file_name": excellon_filename
                    }
                )
                
                # Send success response for Gerber drill map
                self.send_callback(
                    callback_queue=callback_queue,
                    job_id=job_id,
                    agent_name=f"{self.queue_name}_map",
                    result={
                        "success": True,
                        "file_content": gerber_map,
                        "file_name": gerber_map_filename
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

    def generate_excellon_file(self, job_id: str, requirements: List[str], category: str) -> tuple:
        """
        Generate an Excellon drill file based on the requirements
        Returns: (excellon_content, file_name)
        """
        logger.info(f"Generating Excellon drill file for job {job_id}")
        
        # Extract parameters from requirements
        board_width = 100.0  # Default values
        board_height = 100.0
        drill_sizes = []
        via_size = 0.6  # Default via size
        mounting_hole_size = 3.2  # Default mounting hole size
        
        # Process requirements to extract parameters
        for req in requirements:
            # Skip if requirement is None or not a string
            if not req or not isinstance(req, str):
                continue
                
            req_lower = req.lower()
            
            # Look for board dimensions
            dim_match = re.search(r'(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)', req)
            if dim_match:
                board_width = float(dim_match.group(1))
                board_height = float(dim_match.group(2))
            
            # Look for via size
            via_match = re.search(r'via.*?(\d+\.?\d*)', req_lower)
            if via_match:
                via_size = float(via_match.group(1))
            
            # Look for mounting hole size
            mount_match = re.search(r'mount.*?hole.*?(\d+\.?\d*)', req_lower)
            if mount_match:
                mounting_hole_size = float(mount_match.group(1))
            
            # Look for drill sizes
            drill_match = re.findall(r'drill.*?(\d+\.?\d*)', req_lower)
            if drill_match:
                for size in drill_match:
                    size_val = float(size)
                    if size_val not in drill_sizes and size_val > 0.1:  # Avoid duplicates and unusually small sizes
                        drill_sizes.append(size_val)
        
        # If no drill sizes specified, use some common defaults
        if not drill_sizes:
            drill_sizes = [0.3, 0.6, 0.8, 1.0, 1.2]
        
        # Add via and mounting hole sizes if not already in drill_sizes
        if via_size not in drill_sizes:
            drill_sizes.append(via_size)
        if mounting_hole_size not in drill_sizes:
            drill_sizes.append(mounting_hole_size)
        
        # Sort drill sizes
        drill_sizes.sort()
        
        # Generate the Excellon file
        excellon_content = self.create_excellon_file(
            board_width, board_height, drill_sizes, via_size, mounting_hole_size
        )
        
        # Return the Excellon content and filename
        file_name = f"drill.xln"
        return excellon_content, file_name

    def generate_drill_map(self, job_id: str, requirements: List[str], category: str) -> tuple:
        """
        Generate a Gerber drill map file based on the requirements
        Returns: (gerber_map_content, file_name)
        """
        logger.info(f"Generating Gerber drill map for job {job_id}")
        
        # Extract parameters from requirements (same as for Excellon)
        board_width = 100.0  # Default values
        board_height = 100.0
        
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
        
        # Generate the Gerber drill map
        gerber_map_content = self.create_gerber_drill_map(board_width, board_height)
        
        # Return the Gerber drill map content and filename
        file_name = f"drill_map.gbr"
        return gerber_map_content, file_name

    def create_excellon_file(self, width: float, height: float, 
                           drill_sizes: List[float], via_size: float, mounting_hole_size: float) -> str:
        """Create an Excellon drill file with various hole types"""
        excellon = []
        
        # Header
        excellon.append("M48")
        excellon.append("FMAT,2")
        excellon.append("METRIC,TZ")
        
        # Tool definitions
        for i, size in enumerate(drill_sizes):
            excellon.append(f"T{i+1}C{size:.3f}")
        
        # End of header
        excellon.append("%")
        excellon.append("G90")
        excellon.append("G05")
        
        # Generate holes
        # Via holes (smaller)
        via_tool = drill_sizes.index(via_size) + 1
        excellon.append(f"T{via_tool}")
        
        # Create a grid of vias
        via_count = random.randint(30, 100)
        for _ in range(via_count):
            x = random.uniform(5, width - 5)
            y = random.uniform(5, height - 5)
            excellon.append(f"X{x:.3f}Y{y:.3f}")
        
        # Mounting holes (larger)
        mount_tool = drill_sizes.index(mounting_hole_size) + 1
        excellon.append(f"T{mount_tool}")
        
        # Add mounting holes in corners
        margin = mounting_hole_size * 2
        excellon.append(f"X{margin:.3f}Y{margin:.3f}")  # Bottom left
        excellon.append(f"X{width-margin:.3f}Y{margin:.3f}")  # Bottom right
        excellon.append(f"X{margin:.3f}Y{height-margin:.3f}")  # Top left
        excellon.append(f"X{width-margin:.3f}Y{height-margin:.3f}")  # Top right
        
        # Other component holes
        for i, size in enumerate(drill_sizes):
            if size != via_size and size != mounting_hole_size:
                tool = i + 1
                excellon.append(f"T{tool}")
                
                # Add some random component holes
                hole_count = random.randint(5, 20)
                for _ in range(hole_count):
                    x = random.uniform(10, width - 10)
                    y = random.uniform(10, height - 10)
                    excellon.append(f"X{x:.3f}Y{y:.3f}")
        
        # End of program
        excellon.append("T0")
        excellon.append("M30")
        
        return "\n".join(excellon)

    def create_gerber_drill_map(self, width: float, height: float) -> str:
        """Create a Gerber file for the drill map visualization"""
        # Header
        gerber = []
        gerber.append("%FSLAX36Y36*%")  # Format specification
        gerber.append("%MOMM*%")  # Units in mm
        gerber.append("%LPD*%")  # Layer polarity - dark
        
        # Aperture definitions
        gerber.append("%ADD10C,0.100*%")  # Circular aperture for outline
        gerber.append("%ADD11C,0.300*%")  # Circular aperture for small holes
        gerber.append("%ADD12C,3.200*%")  # Circular aperture for mounting holes
        
        # Draw outline
        gerber.append("G01*")  # Linear interpolation
        gerber.append("D10*")  # Select aperture 10
        gerber.append("X0Y0D02*")  # Move to origin
        gerber.append(f"X{int(width * 1000000)}Y0D01*")  # Draw to right
        gerber.append(f"X{int(width * 1000000)}Y{int(height * 1000000)}D01*")  # Draw to top-right
        gerber.append(f"X0Y{int(height * 1000000)}D01*")  # Draw to top-left
        gerber.append("X0Y0D01*")  # Draw to origin
        
        # Draw a drill legend in the bottom left
        gerber.append("G04 Drill Legend*")
        legend_x = int(5 * 1000000)
        legend_y = int(5 * 1000000)
        gerber.append(f"X{legend_x}Y{legend_y}D02*")
        gerber.append(f"X{legend_x + int(20 * 1000000)}Y{legend_y}D01*")
        gerber.append(f"X{legend_x + int(20 * 1000000)}Y{legend_y + int(15 * 1000000)}D01*")
        gerber.append(f"X{legend_x}Y{legend_y + int(15 * 1000000)}D01*")
        gerber.append(f"X{legend_x}Y{legend_y}D01*")
        
        # Draw some text-like markings for the legend
        text_y = legend_y + int(3 * 1000000)
        gerber.append(f"X{legend_x + int(2 * 1000000)}Y{text_y}D02*")
        gerber.append(f"X{legend_x + int(18 * 1000000)}Y{text_y}D01*")
        
        text_y += int(4 * 1000000)
        gerber.append(f"X{legend_x + int(2 * 1000000)}Y{text_y}D02*")
        gerber.append(f"X{legend_x + int(18 * 1000000)}Y{text_y}D01*")
        
        text_y += int(4 * 1000000)
        gerber.append(f"X{legend_x + int(2 * 1000000)}Y{text_y}D02*")
        gerber.append(f"X{legend_x + int(18 * 1000000)}Y{text_y}D01*")
        
        # Add mounting holes
        gerber.append("D12*")  # Select mounting hole aperture
        margin = int(3.2 * 2 * 1000000)
        gerber.append(f"X{margin}Y{margin}D03*")  # Bottom left
        gerber.append(f"X{int(width * 1000000) - margin}Y{margin}D03*")  # Bottom right
        gerber.append(f"X{margin}Y{int(height * 1000000) - margin}D03*")  # Top left
        gerber.append(f"X{int(width * 1000000) - margin}Y{int(height * 1000000) - margin}D03*")  # Top right
        
        # Add via holes
        gerber.append("D11*")  # Select via hole aperture
        
        # Create a grid of vias
        via_count = random.randint(30, 100)
        for _ in range(via_count):
            x = int(random.uniform(5, width - 5) * 1000000)
            y = int(random.uniform(5, height - 5) * 1000000)
            gerber.append(f"X{x}Y{y}D03*")
        
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
    agent = DrillAgent()
    
    # Start health check in a separate thread
    health_thread = threading.Thread(target=health_check_thread, args=(agent,), daemon=True)
    health_thread.start()
    
    # Run the agent
    agent.run()