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

class SoldermaskTopAgent:
    def __init__(self):
        self.queue_name = 'soldermask_top'
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
                
            # Process the requirements and generate a soldermask Gerber file
            try:
                gerber_content, file_name = self.generate_soldermask_file(job_id, requirements, category)
                
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

    def generate_soldermask_file(self, job_id: str, requirements: List[str], category: str) -> tuple:
        """
        Generate a soldermask Gerber file based on the requirements
        Returns: (gerber_content, file_name)
        """
        logger.info(f"Generating soldermask top Gerber for job {job_id}")
        
        # Extract parameters from requirements
        board_width = 100.0  # Default values in mm
        board_height = 100.0
        soldermask_color = "green"  # Default color
        clearance = 0.1  # Default clearance around pads in mm
        min_dam_width = 0.2  # Default minimum dam width between pads in mm
        
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
            
            # Look for soldermask color
            if "color" in req_lower:
                if "green" in req_lower:
                    soldermask_color = "green"
                elif "red" in req_lower:
                    soldermask_color = "red"
                elif "blue" in req_lower:
                    soldermask_color = "blue"
                elif "black" in req_lower:
                    soldermask_color = "black"
                elif "white" in req_lower:
                    soldermask_color = "white"
                elif "purple" in req_lower:
                    soldermask_color = "purple"
                elif "yellow" in req_lower:
                    soldermask_color = "yellow"
            
            # Look for clearance
            clearance_match = re.search(r'clearance.*?(\d+\.?\d*)', req_lower)
            if clearance_match:
                clearance = float(clearance_match.group(1))
            
            # Look for minimum dam width
            dam_match = re.search(r'dam.*?width.*?(\d+\.?\d*)', req_lower)
            if dam_match:
                min_dam_width = float(dam_match.group(1))
        
        # Generate the soldermask Gerber file
        gerber_content = self.create_soldermask_gerber(
            board_width, board_height, soldermask_color, clearance, min_dam_width
        )
        
        # Return the Gerber content and filename
        file_name = f"soldermask_top.gbr"
        return gerber_content, file_name

    def create_soldermask_gerber(self, width: float, height: float, color: str, 
                               clearance: float, min_dam_width: float) -> str:
        """Create a Gerber file for the top soldermask layer"""
        # Header
        gerber = []
        gerber.append("%FSLAX36Y36*%")  # Format specification
        gerber.append("%MOMM*%")  # Units in mm
        gerber.append("%LPD*%")  # Layer polarity - dark
        
        # Add soldermask color in a comment
        gerber.append(f"G04 Soldermask color: {color}*")
        
        # Aperture definitions
        gerber.append("%ADD10C,0.100*%")  # Circular aperture for outlines
        gerber.append("%ADD11C,0.800*%")  # Circular aperture for small pads
        gerber.append("%ADD12R,1.500X1.500*%")  # Rectangular aperture for square pads
        gerber.append("%ADD13R,2.000X1.500*%")  # Rectangular aperture for rectangular pads
        gerber.append("%ADD14C,3.200*%")  # Circular aperture for mounting holes
        gerber.append("%ADD15C,0.600*%")  # Circular aperture for vias
        
        # Draw board outline
        gerber.append("G01*")  # Linear interpolation
        gerber.append("D10*")  # Select aperture 10
        gerber.append("X0Y0D02*")  # Move to origin
        gerber.append(f"X{int(width * 1000000)}Y0D01*")  # Draw to right
        gerber.append(f"X{int(width * 1000000)}Y{int(height * 1000000)}D01*")  # Draw to top-right
        gerber.append(f"X0Y{int(height * 1000000)}D01*")  # Draw to top-left
        gerber.append("X0Y0D01*")  # Draw to origin
        
        # Create a soldermask opening (negative) that covers the entire board
        # This represents the soldermask covering everything by default
        gerber.append("%LPC*%")  # Layer polarity - clear (negative)
        
        # Draw a rectangle covering the entire board to represent the soldermask
        gerber.append("G36*")  # Begin region
        gerber.append("X0Y0D02*")  # Move to origin
        gerber.append(f"X{int(width * 1000000)}Y0D01*")  # Draw to right
        gerber.append(f"X{int(width * 1000000)}Y{int(height * 1000000)}D01*")  # Draw to top-right
        gerber.append(f"X0Y{int(height * 1000000)}D01*")  # Draw to top-left
        gerber.append("X0Y0D01*")  # Draw to origin
        gerber.append("G37*")  # End region
        
        # Switch back to dark polarity for creating openings in the soldermask
        gerber.append("%LPD*%")  # Layer polarity - dark
        
        # Add openings for mounting holes
        gerber.append("D14*")  # Select mounting hole aperture
        margin = 3.2 * 2  # Twice the hole diameter for margin
        gerber.append(f"X{int(margin * 1000000)}Y{int(margin * 1000000)}D03*")  # Bottom left
        gerber.append(f"X{int((width - margin) * 1000000)}Y{int(margin * 1000000)}D03*")  # Bottom right
        gerber.append(f"X{int(margin * 1000000)}Y{int((height - margin) * 1000000)}D03*")  # Top left
        gerber.append(f"X{int((width - margin) * 1000000)}Y{int((height - margin) * 1000000)}D03*")  # Top right
        
        # Add openings for various pads with clearance
        # Round pads
        gerber.append("D11*")  # Select pad aperture
        for i in range(random.randint(30, 60)):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            # Increase the aperture size by clearance for soldermask opening
            pad_diam = 0.8 + clearance * 2
            gerber.append(f"%ADD20C,{pad_diam:.3f}*%")  # Create custom aperture with clearance
            gerber.append("D20*")  # Select custom aperture
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")  # Flash pad
        
        # Square pads
        for i in range(random.randint(20, 40)):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            # Increase the aperture size by clearance for soldermask opening
            pad_width = 1.5 + clearance * 2
            pad_height = 1.5 + clearance * 2
            gerber.append(f"%ADD21R,{pad_width:.3f}X{pad_height:.3f}*%")  # Create custom aperture
            gerber.append("D21*")  # Select custom aperture
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")  # Flash pad
        
        # Rectangular pads
        for i in range(random.randint(15, 30)):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            # Increase the aperture size by clearance for soldermask opening
            pad_width = 2.0 + clearance * 2
            pad_height = 1.5 + clearance * 2
            gerber.append(f"%ADD22R,{pad_width:.3f}X{pad_height:.3f}*%")  # Create custom aperture
            gerber.append("D22*")  # Select custom aperture
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")  # Flash pad
        
        # Add openings for vias
        via_aperture = 0.6 + clearance * 2  # Via diameter plus clearance
        gerber.append(f"%ADD23C,{via_aperture:.3f}*%")  # Create custom aperture for vias
        gerber.append("D23*")  # Select via aperture
        
        # Create a grid of vias
        via_count = random.randint(30, 70)
        for _ in range(via_count):
            x = random.uniform(5, width - 5)
            y = random.uniform(5, height - 5)
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")
        
        # Add some IC outlines with soldermask openings
        self.add_ic_soldermask_openings(gerber, width * 0.3, height * 0.3, 10, 6, clearance)
        self.add_ic_soldermask_openings(gerber, width * 0.7, height * 0.7, 15, 8, clearance)
        
        # End of file
        gerber.append("M02*")
        
        return "\n".join(gerber)

    def add_ic_soldermask_openings(self, gerber: List[str], x: float, y: float, 
                                 width: float, height: float, clearance: float):
        """Add soldermask openings for an IC package"""
        # Create openings for IC pads (typically around the perimeter)
        
        # Define pad size with clearance
        pad_width = 1.2 + clearance * 2
        pad_height = 0.6 + clearance * 2
        gerber.append(f"%ADD30R,{pad_width:.3f}X{pad_height:.3f}*%")  # Create custom aperture
        gerber.append("D30*")  # Select custom aperture
        
        # Bottom row of pads
        num_pads_per_side = min(8, int(width / 1.5))  # Limit the number of pads
        pad_spacing = width / (num_pads_per_side + 1)
        
        for i in range(1, num_pads_per_side + 1):
            pad_x = x + i * pad_spacing
            pad_y = y + 0.5  # Slightly inset from edge
            gerber.append(f"X{int(pad_x * 1000000)}Y{int(pad_y * 1000000)}D03*")
        
        # Top row of pads
        for i in range(1, num_pads_per_side + 1):
            pad_x = x + i * pad_spacing
            pad_y = y + height - 0.5  # Slightly inset from edge
            gerber.append(f"X{int(pad_x * 1000000)}Y{int(pad_y * 1000000)}D03*")
        
        # Rotate the pad aperture 90 degrees for side pads
        pad_width, pad_height = pad_height, pad_width
        gerber.append(f"%ADD31R,{pad_width:.3f}X{pad_height:.3f}*%")  # Create custom aperture
        gerber.append("D31*")  # Select custom aperture
        
        # Left column of pads
        num_pads_side = min(6, int(height / 1.5))  # Limit the number of pads
        pad_spacing = height / (num_pads_side + 1)
        
        for i in range(1, num_pads_side + 1):
            pad_x = x + 0.5  # Slightly inset from edge
            pad_y = y + i * pad_spacing
            gerber.append(f"X{int(pad_x * 1000000)}Y{int(pad_y * 1000000)}D03*")
        
        # Right column of pads
        for i in range(1, num_pads_side + 1):
            pad_x = x + width - 0.5  # Slightly inset from edge
            pad_y = y + i * pad_spacing
            gerber.append(f"X{int(pad_x * 1000000)}Y{int(pad_y * 1000000)}D03*")

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
    agent = SoldermaskTopAgent()
    
    # Start health check in a separate thread
    health_thread = threading.Thread(target=health_check_thread, args=(agent,), daemon=True)
    health_thread.start()
    
    # Run the agent
    agent.run()