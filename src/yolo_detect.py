#!/usr/bin/env python3
"""
YOLO Object Detection for Telegram Medical Images
"""

import os
import cv2
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import numpy as np
from ultralytics import YOLO
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yolo_detection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YOLODetector:
    def __init__(self, model_path: str = 'yolov8n.pt'):
        """
        Initialize YOLO detector
        
        Args:
            model_path: Path to YOLO model weights
        """
        self.model_path = model_path
        self.model = None
        self.results_dir = Path("data/processed/yolo_results")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # COCO class names (YOLOv8 default)
        self.class_names = [
            'person', 'bicycle', 'car', 'motorcycle', 'airplane', 'bus', 'train', 'truck', 'boat',
            'traffic light', 'fire hydrant', 'stop sign', 'parking meter', 'bench', 'bird', 'cat',
            'dog', 'horse', 'sheep', 'cow', 'elephant', 'bear', 'zebra', 'giraffe', 'backpack',
            'umbrella', 'handbag', 'tie', 'suitcase', 'frisbee', 'skis', 'snowboard', 'sports ball',
            'kite', 'baseball bat', 'baseball glove', 'skateboard', 'surfboard', 'tennis racket',
            'bottle', 'wine glass', 'cup', 'fork', 'knife', 'spoon', 'bowl', 'banana', 'apple',
            'sandwich', 'orange', 'broccoli', 'carrot', 'hot dog', 'pizza', 'donut', 'cake', 'chair',
            'couch', 'potted plant', 'bed', 'dining table', 'toilet', 'tv', 'laptop', 'mouse',
            'remote', 'keyboard', 'cell phone', 'microwave', 'oven', 'toaster', 'sink', 'refrigerator',
            'book', 'clock', 'vase', 'scissors', 'teddy bear', 'hair drier', 'toothbrush'
        ]
        
        # Medical product related classes
        self.medical_classes = {
            'bottle': 'container',
            'cup': 'container',
            'bowl': 'container',
            'book': 'documentation',
            'scissors': 'medical_tool',
            'knife': 'medical_tool'
        }
        
        # Initialize model
        self.load_model()
    
    def load_model(self):
        """Load YOLO model"""
        try:
            logger.info(f"Loading YOLO model from {self.model_path}")
            self.model = YOLO(self.model_path)
            logger.info("YOLO model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load YOLO model: {str(e)}")
            raise
    
    def detect_objects(self, image_path: Path) -> Dict:
        """
        Detect objects in an image
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Dictionary with detection results
        """
        try:
            if not image_path.exists():
                logger.error(f"Image not found: {image_path}")
                return {}
            
            # Read image
            image = cv2.imread(str(image_path))
            if image is None:
                logger.error(f"Failed to read image: {image_path}")
                return {}
            
            # Run detection
            results = self.model(image)
            
            # Parse results
            detections = []
            for result in results:
                boxes = result.boxes
                if boxes is not None:
                    for box in boxes:
                        detection = {
                            'class_id': int(box.cls[0]),
                            'class_name': self.class_names[int(box.cls[0])],
                            'confidence': float(box.conf[0]),
                            'bbox': box.xyxy[0].tolist()  # [x1, y1, x2, y2]
                        }
                        detections.append(detection)
            
            # Classify image based on detected objects
            image_category = self.classify_image(detections)
            
            result = {
                'image_path': str(image_path),
                'detections': detections,
                'detection_count': len(detections),
                'image_category': image_category,
                'processing_time': datetime.now().isoformat()
            }
            
            logger.debug(f"Detected {len(detections)} objects in {image_path.name}")
            return result
            
        except Exception as e:
            logger.error(f"Error detecting objects in {image_path}: {str(e)}")
            return {}
    
    def classify_image(self, detections: List[Dict]) -> str:
        """
        Classify image based on detected objects
        
        Args:
            detections: List of detection dictionaries
            
        Returns:
            Image classification category
        """
        if not detections:
            return 'other'
        
        # Extract detected classes
        detected_classes = [d['class_name'] for d in detections]
        detected_classes_set = set(detected_classes)
        
        # Check for medical containers
        medical_containers = ['bottle', 'cup', 'bowl']
        has_medical_container = any(cls in medical_containers for cls in detected_classes_set)
        
        # Check for person
        has_person = 'person' in detected_classes_set
        
        # Check for medical tools
        medical_tools = ['scissors', 'knife']
        has_medical_tool = any(cls in medical_tools for cls in detected_classes_set)
        
        # Classification logic
        if has_person and has_medical_container:
            return 'promotional'
        elif has_medical_container and not has_person:
            return 'product_display'
        elif has_person and not has_medical_container:
            return 'lifestyle'
        elif has_medical_tool:
            return 'medical_tools'
        else:
            return 'other'
    
    def process_images_directory(self, images_dir: Path, output_csv: Path = None) -> List[Dict]:
        """
        Process all images in a directory
        
        Args:
            images_dir: Directory containing images
            output_csv: Path to output CSV file
            
        Returns:
            List of detection results
        """
        if not images_dir.exists():
            logger.error(f"Images directory not found: {images_dir}")
            return []
        
        # Find all image files
        image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.tiff']
        image_files = []
        for ext in image_extensions:
            image_files.extend(images_dir.rglob(f"*{ext}"))
            image_files.extend(images_dir.rglob(f"*{ext.upper()}"))
        
        logger.info(f"Found {len(image_files)} images in {images_dir}")
        
        # Process images
        all_results = []
        processed_count = 0
        
        for image_path in image_files:
            # Extract message_id from filename (format: {message_id}.jpg)
            try:
                message_id = int(image_path.stem)
                channel_name = image_path.parent.name
            except ValueError:
                logger.warning(f"Could not extract message_id from {image_path}")
                continue
            
            logger.info(f"Processing image: {image_path.name}")
            result = self.detect_objects(image_path)
            
            if result:
                # Add metadata
                result['message_id'] = message_id
                result['channel_name'] = channel_name
                result['filename'] = image_path.name
                
                all_results.append(result)
                processed_count += 1
                
                # Log progress
                if processed_count % 10 == 0:
                    logger.info(f"Processed {processed_count}/{len(image_files)} images")
        
        logger.info(f"Completed processing {processed_count} images")
        
        # Save results to CSV
        if output_csv and all_results:
            self.save_results_csv(all_results, output_csv)
        
        # Save results to JSON
        json_path = self.results_dir / f"detections_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.save_results_json(all_results, json_path)
        
        return all_results
    
    def save_results_csv(self, results: List[Dict], output_path: Path):
        """Save detection results to CSV"""
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([
                    'message_id', 'channel_name', 'filename', 'image_path',
                    'detection_count', 'image_category', 'processing_time',
                    'detected_objects', 'confidence_scores'
                ])
                
                # Write data
                for result in results:
                    # Extract detection info
                    detections = result.get('detections', [])
                    object_names = [d['class_name'] for d in detections]
                    confidences = [d['confidence'] for d in detections]
                    
                    writer.writerow([
                        result.get('message_id', ''),
                        result.get('channel_name', ''),
                        result.get('filename', ''),
                        result.get('image_path', ''),
                        result.get('detection_count', 0),
                        result.get('image_category', ''),
                        result.get('processing_time', ''),
                        ';'.join(object_names),
                        ';'.join([str(c) for c in confidences])
                    ])
            
            logger.info(f"Saved CSV results to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving CSV results: {str(e)}")
    
    def save_results_json(self, results: List[Dict], output_path: Path):
        """Save detection results to JSON"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"Saved JSON results to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving JSON results: {str(e)}")
    
    def analyze_results(self, results: List[Dict]):
        """Analyze detection results and generate statistics"""
        if not results:
            logger.warning("No results to analyze")
            return
        
        # Calculate statistics
        total_images = len(results)
        categories = {}
        object_counts = {}
        confidence_scores = []
        
        for result in results:
            # Count categories
            category = result.get('image_category', 'other')
            categories[category] = categories.get(category, 0) + 1
            
            # Count objects
            detections = result.get('detections', [])
            for detection in detections:
                class_name = detection.get('class_name', 'unknown')
                object_counts[class_name] = object_counts.get(class_name, 0) + 1
                confidence_scores.append(detection.get('confidence', 0))
        
        # Generate report
        report = {
            'summary': {
                'total_images_processed': total_images,
                'total_detections': sum(object_counts.values()),
                'avg_confidence': np.mean(confidence_scores) if confidence_scores else 0,
                'processing_date': datetime.now().isoformat()
            },
            'category_distribution': categories,
            'top_objects': dict(sorted(object_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            'confidence_stats': {
                'mean': np.mean(confidence_scores) if confidence_scores else 0,
                'std': np.std(confidence_scores) if confidence_scores else 0,
                'min': min(confidence_scores) if confidence_scores else 0,
                'max': max(confidence_scores) if confidence_scores else 0
            }
        }
        
        # Save report
        report_path = self.results_dir / f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Analysis report saved to {report_path}")
        
        # Print summary
        print("\n" + "="*60)
        print("YOLO DETECTION ANALYSIS SUMMARY")
        print("="*60)
        print(f"Total Images Processed: {total_images}")
        print(f"Total Detections: {sum(object_counts.values())}")
        print(f"Average Confidence: {report['confidence_stats']['mean']:.2%}")
        print("\nCategory Distribution:")
        for category, count in categories.items():
            percentage = (count / total_images) * 100
            print(f"  {category}: {count} ({percentage:.1f}%)")
        
        print("\nTop 10 Detected Objects:")
        for obj, count in report['top_objects'].items():
            print(f"  {obj}: {count}")
        
        return report

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="YOLO Object Detection for Medical Images")
    parser.add_argument("--images-dir", type=str, default="data/raw/images",
                       help="Directory containing images to process")
    parser.add_argument("--model", type=str, default="yolov8n.pt",
                       help="Path to YOLO model weights")
    parser.add_argument("--output", type=str, default=None,
                       help="Output CSV file path")
    
    args = parser.parse_args()
    
    try:
        # Initialize detector
        detector = YOLODetector(args.model)
        
        # Process images
        images_dir = Path(args.images_dir)
        output_csv = Path(args.output) if args.output else None
        
        if not output_csv:
            output_csv = detector.results_dir / f"detections_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        results = detector.process_images_directory(images_dir, output_csv)
        
        # Analyze results
        if results:
            detector.analyze_results(results)
            print(f"\n✓ YOLO detection completed successfully")
            print(f"✓ Results saved to: {output_csv}")
        else:
            print("✗ No images were processed")
        
        return 0
        
    except Exception as e:
        logger.error(f"YOLO detection failed: {str(e)}")
        print(f"✗ YOLO detection failed: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())