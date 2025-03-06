import pika
import json
import logging
import time
import re
import random
from typing import Dict, List, Any, Optional
import threading
import math
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SilkscreenAgent:
    def __init__(self):
        self.queue_name = 'silkscreen'
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
                
            # Process the requirements and generate a silkscreen Gerber file
            try:
                gerber_content, file_name = self.generate_silkscreen_file(job_id, requirements, category)
                
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

    def generate_silkscreen_file(self, job_id: str, requirements: List[str], category: str) -> tuple:
        """
        Generate a silkscreen (legend) Gerber file based on the requirements
        Returns: (gerber_content, file_name)
        """
        logger.info(f"Generating silkscreen Gerber for job {job_id}")
        
        # Extract parameters from requirements
        board_width = 100.0  # Default values in mm
        board_height = 100.0
        text_height = 1.5  # Default text height
        line_width = 0.2  # Default line width
        pcb_name = "CIRCUIT-IQ"  # Default PCB name
        revision = "REV1"  # Default revision
        manufacturer = "CIRCUIT-IQ"  # Default manufacturer
        
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
            
            # Look for text height
            text_match = re.search(r'text.*?height.*?(\d+\.?\d*)', req_lower)
            if text_match:
                text_height = float(text_match.group(1))
            
            # Look for line width
            line_match = re.search(r'line.*?width.*?(\d+\.?\d*)', req_lower)
            if line_match:
                line_width = float(line_match.group(1))
            
            # Look for PCB name
            name_match = re.search(r'name[:\s]+([a-zA-Z0-9_-]+)', req)
            if name_match:
                pcb_name = name_match.group(1)
            
            # Look for revision
            rev_match = re.search(r'rev(?:ision)?[:\s]+([a-zA-Z0-9_\.]+)', req_lower)
            if rev_match:
                revision = rev_match.group(1)
            
            # Look for manufacturer
            mfg_match = re.search(r'manufacturer[:\s]+([a-zA-Z0-9_-]+)', req)
            if mfg_match:
                manufacturer = mfg_match.group(1)
        
        # Generate the silkscreen Gerber file
        gerber_content = self.create_silkscreen_gerber(
            board_width, board_height, text_height, line_width, 
            pcb_name, revision, manufacturer
        )
        
        # Return the Gerber content and filename
        file_name = f"silkscreen_top.gbr"
        return gerber_content, file_name

    def create_silkscreen_gerber(self, width: float, height: float, text_height: float, 
                               line_width: float, pcb_name: str, revision: str, 
                               manufacturer: str) -> str:
        """Create a Gerber file for the silkscreen (legend) layer"""
        # Header
        gerber = []
        gerber.append("%FSLAX36Y36*%")  # Format specification
        gerber.append("%MOMM*%")  # Units in mm
        gerber.append("%LPD*%")  # Layer polarity - dark
        
        # Aperture definitions
        gerber.append(f"%ADD10C,{line_width:.3f}*%")  # Circular aperture for lines
        gerber.append(f"%ADD11C,{line_width * 0.8:.3f}*%")  # Circular aperture for text
        gerber.append(f"%ADD12C,{line_width * 1.5:.3f}*%")  # Circular aperture for component outlines
        
        # Draw board outline
        gerber.append("G01*")  # Linear interpolation
        gerber.append("D10*")  # Select aperture 10
        gerber.append("X0Y0D02*")  # Move to origin
        gerber.append(f"X{int(width * 1000000)}Y0D01*")  # Draw to right
        gerber.append(f"X{int(width * 1000000)}Y{int(height * 1000000)}D01*")  # Draw to top-right
        gerber.append(f"X0Y{int(height * 1000000)}D01*")  # Draw to top-left
        gerber.append("X0Y0D01*")  # Draw to origin
        
        # Add PCB name text at the top center
        gerber.append("D11*")  # Select text aperture
        self.add_text_to_gerber(gerber, pcb_name, width/2, height - 5, text_height)
        
        # Add revision in the bottom right
        self.add_text_to_gerber(gerber, revision, width - 10, 5, text_height * 0.8)
        
        # Add manufacturer in the bottom left
        self.add_text_to_gerber(gerber, manufacturer, 5, 5, text_height * 0.8)
        
        # Add date in the top right (using current date)
        date_str = datetime.now().strftime("%Y-%m-%d")
        self.add_text_to_gerber(gerber, date_str, width - 20, height - 5, text_height * 0.7)
        
        # Add component outlines and reference designators
        gerber.append("D12*")  # Select component outline aperture
        
        # Add some fake ICs
        self.add_ic_outline(gerber, width * 0.3, height * 0.3, 10, 6, text_height)
        self.add_ic_outline(gerber, width * 0.7, height * 0.7, 15, 8, text_height)
        
        # Add some fake resistors
        resistor_count = random.randint(10, 30)
        for i in range(resistor_count):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            rot = random.choice([0, 90])  # 0 or 90 degree rotation
            self.add_resistor_outline(gerber, x, y, rot, f"R{i+1}", text_height * 0.6)
        
        # Add some fake capacitors
        cap_count = random.randint(5, 20)
        for i in range(cap_count):
            x = random.uniform(10, width - 10)
            y = random.uniform(10, height - 10)
            rot = random.choice([0, 90])  # 0 or 90 degree rotation
            self.add_capacitor_outline(gerber, x, y, rot, f"C{i+1}", text_height * 0.6)
        
        # Add orientation marks in the corners
        self.add_orientation_mark(gerber, 3, 3, text_height)  # Bottom left
        self.add_orientation_mark(gerber, width - 3, 3, text_height)  # Bottom right
        self.add_orientation_mark(gerber, 3, height - 3, text_height)  # Top left
        self.add_orientation_mark(gerber, width - 3, height - 3, text_height)  # Top right
        
        # End of file
        gerber.append("M02*")
        
        return "\n".join(gerber)

    def add_text_to_gerber(self, gerber: List[str], text: str, x: float, y: float, height: float):
        """
        Add text to the Gerber file
        This is a simplified text drawing using line segments
        """
        # Simplified ASCII character outlines using line segments
        x_pos = x
        for char in text:
            if char == 'A':
                gerber.append(f"X{int((x_pos) * 1000000)}Y{int(y * 1000000)}D02*")
                gerber.append(f"X{int((x_pos + height/2) * 1000000)}Y{int((y + height) * 1000000)}D01*")
                gerber.append(f"X{int((x_pos + height) * 1000000)}Y{int(y * 1000000)}D01*")
                gerber.append(f"X{int((x_pos + height/4) * 1000000)}Y{int((y + height/2) * 1000000)}D02*")
                gerber.append(f"X{int((x_pos + height*3/4) * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
            elif char in 'BC':
                # Simplified B and C
                gerber.append(f"X{int(x_pos * 1000000)}Y{int(y * 1000000)}D02*")
                gerber.append(f"X{int(x_pos * 1000000)}Y{int((y + height) * 1000000)}D01*")
                
                if char == 'B':
                    # B top half
                    gerber.append(f"X{int(x_pos * 1000000)}Y{int((y + height) * 1000000)}D02*")
                    gerber.append(f"X{int((x_pos + height*2/3) * 1000000)}Y{int((y + height) * 1000000)}D01*")
                    gerber.append(f"X{int((x_pos + height*2/3) * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
                    gerber.append(f"X{int(x_pos * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
                    
                    # B bottom half
                    gerber.append(f"X{int(x_pos * 1000000)}Y{int((y + height/2) * 1000000)}D02*")
                    gerber.append(f"X{int((x_pos + height*3/4) * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
                    gerber.append(f"X{int((x_pos + height*3/4) * 1000000)}Y{int(y * 1000000)}D01*")
                    gerber.append(f"X{int(x_pos * 1000000)}Y{int(y * 1000000)}D01*")
                else:  # C
                    gerber.append(f"X{int(x_pos * 1000000)}Y{int((y + height) * 1000000)}D02*")
                    gerber.append(f"X{int((x_pos + height*2/3) * 1000000)}Y{int((y + height) * 1000000)}D01*")
                    
                    gerber.append(f"X{int(x_pos * 1000000)}Y{int(y * 1000000)}D02*")
                    gerber.append(f"X{int((x_pos + height*2/3) * 1000000)}Y{int(y * 1000000)}D01*")
            # More character definitions can be added here
            # This is a simplification. In reality, we would use a proper font or vector definitions
            
            # For other characters, just draw a simple rectangle
            else:
                gerber.append(f"X{int(x_pos * 1000000)}Y{int(y * 1000000)}D02*")
                gerber.append(f"X{int((x_pos + height/2) * 1000000)}Y{int(y * 1000000)}D01*")
                gerber.append(f"X{int((x_pos + height/2) * 1000000)}Y{int((y + height) * 1000000)}D01*")
                gerber.append(f"X{int(x_pos * 1000000)}Y{int((y + height) * 1000000)}D01*")
                gerber.append(f"X{int(x_pos * 1000000)}Y{int(y * 1000000)}D01*")
            
            # Move to next character position
            x_pos += height * 0.7  # Adjust spacing between characters

    def add_ic_outline(self, gerber: List[str], x: float, y: float, width: float, height: float, text_height: float):
        """Add an IC outline with pin-1 indicator and reference designator"""
        # Draw IC outline
        gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D02*")
        gerber.append(f"X{int((x + width) * 1000000)}Y{int(y * 1000000)}D01*")
        gerber.append(f"X{int((x + width) * 1000000)}Y{int((y + height) * 1000000)}D01*")
        gerber.append(f"X{int(x * 1000000)}Y{int((y + height) * 1000000)}D01*")
        gerber.append(f"X{int(x * 1000000)}Y{int(y * 1000000)}D01*")
        
        # Draw pin-1 indicator (circle in top left)
        gerber.append(f"X{int((x + 1.5) * 1000000)}Y{int((y + height - 1.5) * 1000000)}D02*")
        rad = 0.75  # radius in mm
        for angle in range(0, 360, 30):  # Draw circle in 30 degree segments
            rad_angle = math.radians(angle)
            x_end = x + 1.5 + rad * math.cos(rad_angle)
            y_end = y + height - 1.5 + rad * math.sin(rad_angle)
            gerber.append(f"X{int(x_end * 1000000)}Y{int(y_end * 1000000)}D01*")
        
        # Add reference designator text
        ic_num = random.randint(1, 20)
        self.add_text_to_gerber(gerber, f"U{ic_num}", x + width/3, y + height/2, text_height * 0.8)

    def add_resistor_outline(self, gerber: List[str], x: float, y: float, rotation: int, ref_des: str, text_height: float):
        """Add a resistor outline with reference designator"""
        width = 3.0  # Standard resistor length
        height = 1.5  # Standard resistor width
        
        if rotation == 90:
            # Swap width and height for 90-degree rotation
            width, height = height, width
        
        # Draw resistor outline
        gerber.append(f"X{int((x - width/2) * 1000000)}Y{int((y - height/2) * 1000000)}D02*")
        gerber.append(f"X{int((x + width/2) * 1000000)}Y{int((y - height/2) * 1000000)}D01*")
        gerber.append(f"X{int((x + width/2) * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
        gerber.append(f"X{int((x - width/2) * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
        gerber.append(f"X{int((x - width/2) * 1000000)}Y{int((y - height/2) * 1000000)}D01*")
        
        # Add reference designator text
        text_x = x if rotation == 0 else x + height/2 + text_height
        text_y = y + height/2 + text_height if rotation == 0 else y
        self.add_text_to_gerber(gerber, ref_des, text_x, text_y, text_height)

    def add_capacitor_outline(self, gerber: List[str], x: float, y: float, rotation: int, ref_des: str, text_height: float):
        """Add a capacitor outline with reference designator"""
        width = 2.0  # Standard capacitor length
        height = 1.2  # Standard capacitor width
        
        if rotation == 90:
            # Swap width and height for 90-degree rotation
            width, height = height, width
        
        # Draw capacitor outline (rectangle with side markers for polarity)
        gerber.append(f"X{int((x - width/2) * 1000000)}Y{int((y - height/2) * 1000000)}D02*")
        gerber.append(f"X{int((x + width/2) * 1000000)}Y{int((y - height/2) * 1000000)}D01*")
        gerber.append(f"X{int((x + width/2) * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
        gerber.append(f"X{int((x - width/2) * 1000000)}Y{int((y + height/2) * 1000000)}D01*")
        gerber.append(f"X{int((x - width/2) * 1000000)}Y{int((y - height/2) * 1000000)}D01*")
        
        # Add polarity marker (+ symbol) for electrolytic capacitors (50% chance)
        if random.random() > 0.5:
            if rotation == 0:
                # Horizontal orientation
                gerber.append(f"X{int((x - width/3) * 1000000)}Y{int(y * 1000000)}D02*")
                gerber.append(f"X{int((x - width/6) * 1000000)}Y{int(y * 1000000)}D01*")
                gerber.append(f"X{int((x - width/4) * 1000000)}Y{int((y + height/6) * 1000000)}D02*")
                gerber.append(f"X{int((x - width/4) * 1000000)}Y{int((y - height/6) * 1000000)}D01*")
            else:
                # Vertical orientation
                gerber.append(f"X{int(x * 1000000)}Y{int((y + width/3) * 1000000)}D02*")
                gerber.append(f"X{int(x * 1000000)}Y{int((y + width/6) * 1000000)}D01*")
                gerber.append(f"X{int((x + height/6) * 1000000)}Y{int((y + width/4) * 1000000)}D02*")
                gerber.append(f"X{int((x - height/6) * 1000000)}Y{int((y + width/4) * 1000000)}D01*")
        
        # Add reference designator text
        text_x = x if rotation == 0 else x + height/2 + text_height
        text_y = y + height/2 + text_height if rotation == 0 else y
        self.add_text_to_gerber(gerber, ref_des, text_x, text_y, text_height)

    def add_orientation_mark(self, gerber: List[str], x: float, y: float, size: float):
        """Add an orientation mark (cross or target) at a specific location"""
        # Draw a cross
        gerber.append(f"X{int((x - size/2) * 1000000)}Y{int(y * 1000000)}D02*")
        gerber.append(f"X{int((x + size/2) * 1000000)}Y{int(y * 1000000)}D01*")
        
        gerber.append(f"X{int(x * 1000000)}Y{int((y - size/2) * 1000000)}D02*")
        gerber.append(f"X{int(x * 1000000)}Y{int((y + size/2) * 1000000)}D01*")
        
        # Draw a circle around the cross
        radius = size * 0.7
        for angle in range(0, 360, 20):  # Draw circle in 20 degree segments
            rad_angle = math.radians(angle)
            next_angle = math.radians((angle + 20) % 360)
            
            x_start = x + radius * math.cos(rad_angle)
            y_start = y + radius * math.sin(rad_angle)
            x_end = x + radius * math.cos(next_angle)
            y_end = y + radius * math.sin(next_angle)
            
            gerber.append(f"X{int(x_start * 1000000)}Y{int(y_start * 1000000)}D02*")
            gerber.append(f"X{int(x_end * 1000000)}Y{int(y_end * 1000000)}D01*")

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
    agent = SilkscreenAgent()
    
    # Start health check in a separate thread
    health_thread = threading.Thread(target=health_check_thread, args=(agent,), daemon=True)
    health_thread.start()
    
    # Run the agent
    agent.run()