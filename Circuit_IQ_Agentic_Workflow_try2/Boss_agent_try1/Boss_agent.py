from fastapi import FastAPI, UploadFile, HTTPException, File, Form, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
import uuid
import pika
import json
from typing import Dict, List, Optional, Any
import spacy
import logging
from io import StringIO, BytesIO
import pandas as pd
import zipfile
import os
import asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Circuit IQ - Boss Agent")

try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    logger.error("Spacy model 'en_core_web_sm' not found. Please install it using 'python -m spacy download en_core_web_sm'")
    raise

class JobStatus(BaseModel):
    job_id: str
    status: str
    completed_tasks: List[str]
    pending_tasks: List[str]
    errors: List[str] = []
    extracted_requirements: Dict = {}
    output_file_path: Optional[str] = None
    creation_time: str

class AgentResult(BaseModel):
    success: bool
    error: Optional[str] = None
    file_content: Optional[str] = None
    file_name: Optional[str] = None

class JobManager:
    def __init__(self):
        self.jobs: Dict[str, JobStatus] = {}
        self.temp_dir = "temp_gerber_files"
        os.makedirs(self.temp_dir, exist_ok=True)
        
        try:
            # Use connection parameters with heartbeat
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    connection_attempts=10,
                    retry_delay=5,
                    heartbeat=600  # 10 minute heartbeat
                )
            )
            self.channel = self.connection.channel()
            
            # Declare exchanges for better message routing
            self.channel.exchange_declare(exchange='gerber_processing', exchange_type='direct', durable=True)
            
            # Declare queues with dead letter exchange
            self.channel.exchange_declare(exchange='gerber_dlx', exchange_type='direct', durable=True)
            
            queue_arguments = {
                'x-dead-letter-exchange': 'gerber_dlx',
                'x-message-ttl': 3600000  # 1 hour TTL
            }
            
            # Declare queues for each agent - use the exact names from worker agents
            self.queues = [
                'copper_top', 'copper_bottom',
                'soldermask_top', 'soldermask_bottom',
                'silkscreen', 'drill'
            ]
            
            for queue in self.queues:
                self.channel.queue_declare(
                    queue=queue, 
                    durable=True,
                    arguments=queue_arguments
                )
                self.channel.queue_bind(
                    exchange='gerber_processing',
                    queue=queue,
                    routing_key=queue
                )
                
            # Declare callback queue with same properties
            self.callback_queue = 'boss_callback'
            self.channel.queue_declare(
                queue=self.callback_queue, 
                durable=True,
                arguments=queue_arguments
            )
            
            # Set up the callback consumer properly
            self.channel.basic_consume(
                queue=self.callback_queue,
                on_message_callback=self.process_callback,
                auto_ack=True
            )
            
            # Store the consumer tag for cleanup
            self.consumer_tag = None
            
            # Start listening for callbacks in a separate thread
            self.start_callback_consumer()
                
        except Exception as e:
            logger.error(f"Failed to initialize RabbitMQ connection: {e}")
            raise

    def __del__(self):
        """Cleanup connections on deletion"""
        try:
            if hasattr(self, 'channel') and self.channel:
                if self.consumer_tag:
                    self.channel.basic_cancel(self.consumer_tag)
                self.channel.close()
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connections: {e}")

    def start_callback_consumer(self):
        """Start consuming messages from the callback queue in a background thread"""
        import threading
        
        def callback_consumer():
            try:
                # Store the consumer tag for cleanup
                self.consumer_tag = self.channel.basic_consume(
                    queue=self.callback_queue,
                    on_message_callback=self.process_callback,
                    auto_ack=True
                )
                self.channel.start_consuming()
            except Exception as e:
                logger.error(f"Error in callback consumer: {e}")
        
        self.consumer_thread = threading.Thread(target=callback_consumer, daemon=True)
        self.consumer_thread.start()

    def process_callback(self, ch, method, properties, body):
        """Process callback messages from worker agents"""
        try:
            message = json.loads(body)
            job_id = message.get("job_id")
            agent_name = message.get("agent_name")
            result = message.get("result", {})
            
            if not job_id or not agent_name:
                logger.error(f"Invalid callback message: {message}")
                return
                
            self.update_job_status(job_id, agent_name, result)
            
        except Exception as e:
            logger.error(f"Error processing callback: {e}")

    def update_job_status(self, job_id: str, agent_name: str, result: Dict[str, Any]):
        """Update job status based on agent callback"""
        if job_id not in self.jobs:
            logger.error(f"Job {job_id} not found in callback from {agent_name}")
            return
            
        job = self.jobs[job_id]
        
        if result.get("success"):
            job.completed_tasks.append(agent_name)
            if agent_name in job.pending_tasks:
                job.pending_tasks.remove(agent_name)
                
            # Save generated file if provided
            if result.get("file_content") and result.get("file_name"):
                file_path = os.path.join(self.temp_dir, f"{job_id}_{result['file_name']}")
                # Write binary content if it's a Gerber file
                with open(file_path, "w") as f:
                    f.write(result["file_content"])
                logger.info(f"Saved file from {agent_name} for job {job_id}: {file_path}")
        else:
            error_msg = f"{agent_name}: {result.get('error', 'Unknown error')}"
            job.errors.append(error_msg)
            logger.error(error_msg)
        
        # Check if all tasks are completed
        if not job.pending_tasks:
            if not job.errors:
                # Create ZIP file with all generated Gerber files
                job.status = "completed"
                self.create_gerber_zip(job_id)
            else:
                job.status = "failed"
            
            # Log completion
            logger.info(f"Job {job_id} {job.status} with {len(job.completed_tasks)} completed tasks and {len(job.errors)} errors")

    def create_job(self) -> str:
        """Create a new job and return its ID"""
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = JobStatus(
            job_id=job_id,
            status="created",
            completed_tasks=[],
            pending_tasks=[],  # Will be updated when tasks are actually sent
            creation_time=datetime.now().isoformat()
        )
        return job_id

    def distribute_tasks(self, job_id: str, files: Dict[str, bytes]):
        """Distribute files to appropriate agent queues"""
        if job_id not in self.jobs:
            raise HTTPException(status_code=404, detail="Job not found")
        
        self.jobs[job_id].status = "processing"
        
        try:
            # Update pending tasks to only include tasks we're actually sending
            self.jobs[job_id].pending_tasks = [
                queue_name for queue_name in self.queues 
                if queue_name in files
            ]
            
            # Distribute each file to its corresponding agent
            for queue_name in self.queues:
                if queue_name in files:
                    message = {
                        "job_id": job_id,
                        "file_content": files[queue_name].decode('utf-8'),
                        "callback_queue": self.callback_queue
                    }
                    self.channel.basic_publish(
                        exchange='gerber_processing',
                        routing_key=queue_name,
                        body=json.dumps(message),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # make message persistent
                            content_type='application/json',
                            reply_to=self.callback_queue
                        )
                    )
                    logger.info(f"Task sent to {queue_name} for job {job_id}")
            
            if not self.jobs[job_id].pending_tasks:
                self.jobs[job_id].status = "completed"
                logger.info(f"Job {job_id} completed immediately (no tasks to process)")
                
        except Exception as e:
            logger.error(f"Error distributing tasks for job {job_id}: {e}")
            self.jobs[job_id].status = "failed"
            self.jobs[job_id].errors.append(f"Task distribution failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to distribute tasks")

    def process_datasheet(self, text: str) -> Dict:
        """Process datasheet using NLP to extract requirements"""
        try:
            doc = nlp(text)
            
            requirements = {
                'copper_layers': [],
                'soldermask': [],
                'silkscreen': [],
                'drill_specs': [],
                'board_dimensions': [],
                'layer_stack': []
            }
            
            # Enhanced rule-based extraction
            for sent in doc.sents:
                sent_lower = sent.text.lower()
                
                # Extract board dimensions
                if any(term in sent_lower for term in ['dimension', 'size', 'width', 'height']):
                    requirements['board_dimensions'].append(sent.text)
                
                # Extract layer stack information
                elif any(term in sent_lower for term in ['layer stack', 'stackup', 'thickness']):
                    requirements['layer_stack'].append(sent.text)
                
                # Existing categories
                elif any(term in sent_lower for term in ['copper', 'cu', 'trace', 'track']):
                    requirements['copper_layers'].append(sent.text)
                elif any(term in sent_lower for term in ['solder', 'mask', 'sm']):
                    requirements['soldermask'].append(sent.text)
                elif any(term in sent_lower for term in ['silk', 'screen', 'legend']):
                    requirements['silkscreen'].append(sent.text)
                elif any(term in sent_lower for term in ['drill', 'hole', 'via']):
                    requirements['drill_specs'].append(sent.text)
            
            return requirements
            
        except Exception as e:
            logger.error(f"Error processing datasheet: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Error processing datasheet: {str(e)}"
            )

    def analyze_and_distribute(self, job_id: str, datasheet_content: str):
        """Analyze datasheet and distribute tasks based on requirements"""
        try:
            requirements = self.process_datasheet(datasheet_content)
            
            # Store requirements in job status
            self.jobs[job_id].extracted_requirements = requirements
            
            # Update the job's pending_tasks list to track which agents we're sending tasks to
            needed_agents = set()
            
            # Create messages for each relevant agent
            for category, specs in requirements.items():
                if specs:
                    # Determine which agents need the requirements based on category
                    queue_mapping = {
                        'copper_layers': ['copper_top', 'copper_bottom'],
                        'soldermask': ['soldermask_top', 'soldermask_bottom'],
                        'silkscreen': ['silkscreen'],
                        'drill_specs': ['drill'],
                        'board_dimensions': ['copper_top', 'copper_bottom', 'soldermask_top', 'soldermask_bottom', 'silkscreen', 'drill'],
                        'layer_stack': ['copper_top', 'copper_bottom', 'soldermask_top', 'soldermask_bottom']
                    }
                    
                    # Add all needed agents to our set
                    for queue in queue_mapping.get(category, []):
                        needed_agents.add(queue)
            
            # Update pending tasks list
            self.jobs[job_id].pending_tasks = list(needed_agents)
            
            # If no agents are needed, mark the job as failed
            if not needed_agents:
                self.jobs[job_id].status = "failed"
                self.jobs[job_id].errors.append("No requirements could be extracted from the datasheet")
                return
                
            # Now send messages to all needed agents
            for agent in needed_agents:
                # Gather all requirements that apply to this agent
                agent_requirements = []
                
                for category, specs in requirements.items():
                    queue_mapping = {
                        'copper_layers': ['copper_top', 'copper_bottom'],
                        'soldermask': ['soldermask_top', 'soldermask_bottom'],
                        'silkscreen': ['silkscreen'],
                        'drill_specs': ['drill'],
                        'board_dimensions': ['copper_top', 'copper_bottom', 'soldermask_top', 'soldermask_bottom', 'silkscreen', 'drill'],
                        'layer_stack': ['copper_top', 'copper_bottom', 'soldermask_top', 'soldermask_bottom']
                    }
                    
                    if agent in queue_mapping.get(category, []) and specs:
                        agent_requirements.extend(specs)
                
                # Only send a message if there are requirements for this agent
                if agent_requirements:
                    message = {
                        "job_id": job_id,
                        "requirements": agent_requirements,
                        "category": "datasheet",
                        "callback_queue": self.callback_queue
                    }
                    
                    try:
                        self.channel.basic_publish(
                            exchange='gerber_processing',
                            routing_key=agent,
                            body=json.dumps(message),
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # make message persistent
                                content_type='application/json'
                            )
                        )
                        logger.info(f"Requirements sent to {agent} for job {job_id}")
                    except Exception as e:
                        logger.error(f"Failed to publish message to {agent}: {e}")
                        self.jobs[job_id].errors.append(f"Failed to distribute task to {agent}")
                        
            # Update job status
            self.jobs[job_id].status = "processing"
                            
        except Exception as e:
            logger.error(f"Error in analyze_and_distribute: {e}")
            self.jobs[job_id].errors.append(str(e))
            self.jobs[job_id].status = "failed"

    def create_gerber_zip(self, job_id: str):
        """Create a ZIP file containing all Gerber files for the job"""
        try:
            if job_id not in self.jobs:
                logger.error(f"Job {job_id} not found when creating ZIP")
                return
                
            # Find all files for this job
            job_files = [f for f in os.listdir(self.temp_dir) if f.startswith(f"{job_id}_")]
            
            if not job_files:
                logger.warning(f"No files found for job {job_id}")
                self.jobs[job_id].errors.append("No Gerber files were generated")
                return
                
            # Create ZIP file
            zip_path = os.path.join(self.temp_dir, f"{job_id}_gerber_files.zip")
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file in job_files:
                    if not file.endswith('.zip'):  # Don't include the ZIP itself
                        file_path = os.path.join(self.temp_dir, file)
                        arcname = file.replace(f"{job_id}_", "")  # Remove job_id prefix in ZIP
                        zipf.write(file_path, arcname)
            
            # Store the ZIP path in the job status
            self.jobs[job_id].output_file_path = zip_path
            logger.info(f"Created ZIP file for job {job_id}: {zip_path}")
            
        except Exception as e:
            logger.error(f"Error creating ZIP file for job {job_id}: {e}")
            self.jobs[job_id].errors.append(f"Failed to create ZIP file: {str(e)}")
            self.jobs[job_id].status = "failed"

# Initialize job manager as part of the application state
@app.on_event("startup")
async def startup_event():
    """Initialize application state"""
    app.state.job_manager = JobManager()

@app.post("/submit")
async def submit_job(
    copper_top: Optional[UploadFile] = None,
    copper_bottom: Optional[UploadFile] = None,
    soldermask_top: Optional[UploadFile] = None,
    soldermask_bottom: Optional[UploadFile] = None,
    silkscreen: Optional[UploadFile] = None,
    drill: Optional[UploadFile] = None
):
    """Submit Gerber files for processing"""
    files = {}
    
    # Read all uploaded files
    for name, file in {
        'copper_top': copper_top,
        'copper_bottom': copper_bottom,
        'soldermask_top': soldermask_top,
        'soldermask_bottom': soldermask_bottom,
        'silkscreen': silkscreen,
        'drill': drill
    }.items():
        if file:
            content = await file.read()
            files[name] = content

    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    # Create new job
    job_id = app.state.job_manager.create_job()
    
    # Distribute tasks
    app.state.job_manager.distribute_tasks(job_id, files)

    return {"job_id": job_id, "status": "processing", "message": "Files submitted successfully"}

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    """Get job status"""
    if job_id not in app.state.job_manager.jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return app.state.job_manager.jobs[job_id]

@app.get("/download/{job_id}")
async def download_zip(job_id: str):
    """Download the generated Gerber ZIP file"""
    if job_id not in app.state.job_manager.jobs:
        raise HTTPException(status_code=404, detail="Job not found")
        
    job = app.state.job_manager.jobs[job_id]
    
    if job.status != "completed":
        raise HTTPException(status_code=400, detail="Job is not completed yet")
        
    if not job.output_file_path or not os.path.exists(job.output_file_path):
        raise HTTPException(status_code=404, detail="Output file not found")
        
    return FileResponse(
        job.output_file_path,
        media_type="application/zip",
        filename=f"gerber_files_{job_id}.zip"
    )

@app.post("/submit-datasheet")
async def submit_datasheet(
    background_tasks: BackgroundTasks,
    datasheet: UploadFile = File(...),
):
    """Submit datasheet for processing"""
    try:
        # Read datasheet content
        datasheet_content = (await datasheet.read()).decode('utf-8')
        
        # Create new job
        job_id = app.state.job_manager.create_job()
        
        # Process datasheet and distribute tasks in background to avoid timeout
        background_tasks.add_task(app.state.job_manager.analyze_and_distribute, job_id, datasheet_content)
        
        return JSONResponse(
            status_code=202,  # Accepted
            content={
                "job_id": job_id,
                "status": "processing",
                "message": "Datasheet analysis initiated"
            }
        )
        
    except Exception as e:
        logger.error(f"Error in submit_datasheet: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing datasheet: {str(e)}"
        )

@app.get("/cleanup")
async def cleanup_old_jobs(days: int = 7):
    """Clean up old jobs and their files"""
    try:
        cutoff_time = datetime.now()
        deleted_count = 0
        
        for job_id, job in list(app.state.job_manager.jobs.items()):
            job_time = datetime.fromisoformat(job.creation_time)
            days_old = (cutoff_time - job_time).days
            
            if days_old >= days:
                # Delete job files
                job_files = [f for f in os.listdir(app.state.job_manager.temp_dir) if f.startswith(f"{job_id}_")]
                for file in job_files:
                    file_path = os.path.join(app.state.job_manager.temp_dir, file)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                
                # Remove job from manager
                del app.state.job_manager.jobs[job_id]
                deleted_count += 1
        
        return {"message": f"Cleaned up {deleted_count} jobs older than {days} days"}
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")
        raise HTTPException(status_code=500, detail=f"Error during cleanup: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown"""
    try:
        # Clean up job manager
        if hasattr(app.state, 'job_manager'):
            del app.state.job_manager
        
        # Clean up temporary files
        temp_dir = "temp_gerber_files"
        if os.path.exists(temp_dir):
            for file in os.listdir(temp_dir):
                try:
                    os.remove(os.path.join(temp_dir, file))
                except Exception as e:
                    logger.error(f"Failed to remove file {file}: {e}")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")