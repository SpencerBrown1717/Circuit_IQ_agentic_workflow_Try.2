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

class CopperBottomAgent:
    def __init__(self):
        self.queue_name = 'copper_bottom'
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
        logger.info(f"Generating copper bottom layer Gerber for job {job_id}")
        
        # Extract relevant parameters from requirements
        board_width = 100.0  # Default values in mm
        board_height = 100.0
        trace_width = 0.2  # Default trace width in mm
        clearance = 0.2  # Default clearance between traces in mm
        copper_weight = 1  # Default copper weight in oz
        via_diameter = 0.6  # Default via diameter in mm
        
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
                
            # Look for trace width
            trace_match = re.search(r'trace.*?width.*?(\d+\.?\d*)', req_lower)
            if trace_match:
                trace_width = float(trace_match.group(1))
                
            # Look for clearance
            clearance_match = re.search(r'clearance.*?(\d+\.?\d*)', req_lower)
            if clearance_match:
                clearance = float(clearance_match.group(1))
                
            # Look for copper weight
            copper_match = re.search(r'copper.*?(\d+\.?\d*)\s*(?:oz|ounce)', req_lower)
            if copper_match:
                copper_weight = float(copper_match.group(1))
                
            # Look for via diameter
            via_match = re.search(r'via.*?diameter.*?(\d+\.?\d*)', req_lower)
            if via_match:
                via_diameter = float(via_match.group(1))
        
        # Generate the Gerber file for copper bottom
        gerber_content = self.create_gerber_copper_bottom(
            board_width, board_height, trace_width, clearance, copper_weight, via_diameter
        )
        
        # Return the Gerber content and filename
        file_name = f"copper_bottom.gbr"
        return gerber_content, file_name

    def create_gerber_copper_bottom(self, width: float, height: float, trace_width: float, 
                                  clearance: float, copper_weight: float, via_diameter: float) -> str:
        """Create a Gerber file for the copper bottom layer with traces and pads"""
        # Header
        gerber = []
        gerber.append("%FSLAX36Y36*%")  # Format specification
        gerber.append("%MOMM*%")  # Units in mm
        gerber.append("%LPD*%")  # Layer polarity - dark
        
        # Add copper weight information in a comment
        gerber.append(f"G04 Copper Bottom Layer, {copper_weight}oz copper*")
        
        # Aperture definitions
        gerber.append(f"%ADD10C,{trace_width:.3f}*%")  # Circular aperture for traces
        gerber.append("%ADD11C,1.000*%")  # Circular aperture for round pads
        gerber.append("%ADD12R,1.500X1.500*%")  # Rectangular aperture for square pads
        gerber.append("%ADD13R,2.000X1.500*%")  # Rectangular aperture for rectangular pads
        gerber.append(f"%ADD14C,{via_diameter:.3f}*%")  # Circular aperture for vias
        gerber.append("%ADD15C,3.200*%")  # Circular aperture for mounting holes
        
        # Draw board outline
        gerber.append("G01*")  # Linear interpolation
        gerber.append("D10*")  # Select aperture 10
        gerber.append("X0Y0D02*")  # Move to origin
        gerber.append(f"X{int(width * 1000000)}Y0D01*")  # Draw to right
        gerber.append(f"X{int(width * 1000000)}Y{int(height * 1000000)}D01*")  # Draw to top-right
        gerber.append(f"X0Y{int(height * 1000000)}D01*")  # Draw to top-left
        gerber.append("X0Y0D01*")  # Draw to origin
        
        # Draw a ground plane or thermal relief if specified
        # For this example, we'll create a ground plane with thermal reliefs around vias
        if copper_weight >= 1.0:  # Only add ground plane for heavier copper
            self.add_ground_plane(gerber, width, height, clearance)
        
        # Add mounting holes
        gerber.append("D15*")  # Select mounting hole aperture
        margin = 3.2 * 2  # Twice the hole diameter for margin
        gerber.append(f"X{int(margin * 1000000)}Y{int(margin * 1000000)}D03*")  # Bottom left
        gerber.append(f"X{int((width - margin) * 1000000)}Y{int(margin * 1000000)}D03*")  # Bottom right
        gerber.append(f"X{int(margin * 1000000)}Y{int((height - margin) * 1000000)}D03*")  # Top left
        gerber.append(f"X{int((width - margin) * 1000000)}Y{int((height - margin) * 1000000)}D03*")  # Top right
        
        # Add vias between top and bottom layers
        gerber.append("D14*")  # Select via aperture
        
        # Create a grid of vias
        via_count = random.randint(30, 70)
        via_positions = []
        for _ in range(via_count):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            via_positions.append((x, y))
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")
        
        # Add component pads
        # Round pads
        gerber.append("D11*")  # Select round pad aperture
        for i in range(random.randint(20, 40)):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")
        
        # Square pads
        gerber.append("D12*")  # Select square pad aperture
        for i in range(random.randint(15, 30)):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")
        
        # Rectangular pads
        gerber.append("D13*")  # Select rectangular pad aperture
        for i in range(random.randint(10, 20)):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D03*")
        
        # Add traces to connect various pads and vias
        gerber.append("D10*")  # Select trace aperture
        
        # Create some random traces
        # We'll make the bottom layer slightly different from top layer by using more horizontal traces
        trace_count = random.randint(40, 80)
        
        # Generate a list of all pad positions (including vias) to connect
        all_positions = via_positions.copy()
        
        # Add horizontal and vertical bus lines (typical for bottom layer)
        # Horizontal buses
        for i in range(3, 8):
            y_pos = height * i / 10
            gerber.append(f"X{int(5 * 1000000)}Y{int(y_pos * 1000000)}D02*")
            gerber.append(f"X{int((width - 5) * 1000000)}Y{int(y_pos * 1000000)}D01*")
            
            # Add some branches from the bus
            branch_count = random.randint(3, 8)
            for j in range(branch_count):
                x_branch = random.uniform(10, width - 10)
                if j % 2 == 0:  # Alternate up and down branches
                    y_end = y_pos + random.uniform(5, 15)
                else:
                    y_end = y_pos - random.uniform(5, 15)
                
                if y_end > 5 and y_end < height - 5:  # Keep within board bounds
                    gerber.append(f"X{int(x_branch * 1000000)}Y{int(y_pos * 1000000)}D02*")
                    gerber.append(f"X{int(x_branch * 1000000)}Y{int(y_end * 1000000)}D01*")
        
        # Connect some random points (mimicking actual PCB traces)
        for _ in range(trace_count):
            if len(all_positions) < 2:
                break
                
            # Get two random points to connect
            start_idx = random.randint(0, len(all_positions) - 1)
            end_idx = random.randint(0, len(all_positions) - 1)
            
            if start_idx != end_idx:
                start_x, start_y = all_positions[start_idx]
                end_x, end_y = all_positions[end_idx]
                
                # Draw trace with 90-degree routing (typical for bottom layer)
                gerber.append(f"X{int(start_x * 1000000)}Y{int(start_y * 1000000)}D02*")
                
                # For bottom layer, prefer to go horizontal first (different from top layer)
                gerber.append(f"X{int(end_x * 1000000)}Y{int(start_y * 1000000)}D01*")
                gerber.append(f"X{int(end_x * 1000000)}Y{int(end_y * 1000000)}D01*")
        
        # End of file
        gerber.append("M02*")
        
        return "\n".join(gerber)

    def add_ground_plane(self, gerber: List[str], width: float, height: float, clearance: float):
        """Add a ground plane with thermal reliefs"""
        # Define a special aperture for thermal reliefs
        gerber.append("%ADD100C,0.100*%")  # Thin aperture for thermal spokes
        
        # We'll create a cross-hatched ground plane
        # This is a simplified approach - in production, ground planes are often created 
        # by filling the entire area and then clearing around traces and pads
        
        # Add hatching in X direction
        gerber.append("D100*")  # Select thermal aperture
        
        # Draw horizontal lines
        step = 2.0  # 2mm spacing for the hatching
        for y in range(0, int(height), int(step)):
            gerber.append(f"X0Y{int(y * 1000000)}D02*")
            gerber.append(f"X{int(width * 1000000)}Y{int(y * 1000000)}D01*")
        
        # Draw vertical lines
        for x in range(0, int(width), int(step)):
            gerber.append(f"X{int(x * 1000000)}Y0D02*")
            gerber.append(f"X{int(x * 1000000)}Y{int(height * 1000000)}D01*")
            
        # In a real PCB design, we would create proper thermal reliefs around pads and vias
        # That requires more complex Gerber generation with proper clearances
        # For this example, the cross-hatching serves as a simplified representation

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
    agent = CopperBottomAgent()
    
    # Start health check in a separate thread
    health_thread = threading.Thread(target=health_check_thread, args=(agent,), daemon=True)
    health_thread.start()
    
    # Run the agent
    agent.run()