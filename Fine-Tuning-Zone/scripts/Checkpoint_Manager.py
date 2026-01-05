"""

-Arman Bazarchi-
Fine-Tuning Zone — Checkpoint Manager


This module implements a checkpoint management system for fine-tuned models,
addressing Constraint 4 (New Zones) and Constraint 11 (Reporting and Visualization)
from the ADSDB project guidelines.

In machine learning experimentation, checkpoint management is essential for:
1. **Reproducibility** (Constraint 9): Enabling researchers to reload and 
   reuse specific model states for comparison and validation.
2. **Experiment Tracking**: Maintaining a history of training runs with their
   configurations, losses, and timestamps.
3. **Model Selection**: Facilitating selection of the best-performing model
   based on objective criteria (e.g., lowest training loss).
 
The checkpoint manager follows the principle of "active checkpoint" selection,
allowing users to explicitly choose which fine-tuned model to use in downstream
tasks (Exploitation Zone). This design supports:

- **A/B Testing**: Compare baseline CLIP vs fine-tuned variants
- **Iterative Refinement**: Test multiple LoRA configurations (r=4, 8, 16, etc.)
- **Version Control**: Track model evolution across training sessions

CHECKPOINT STRUCTURE
--------------------------------------------------------------------------------
Each checkpoint directory contains:
  - training_config.json: Hyperparameters, training history, metadata
  - text_encoder/: LoRA adapter weights for CLIP text encoder
  - vision_encoder/: LoRA adapter weights for CLIP vision encoder

This structure aligns with the PEFT (Parameter-Efficient Fine-Tuning) library
format, enabling seamless integration with Hugging Face Transformers.

"""

import os
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# The active checkpoint configuration is persisted in a JSON file, allowing
# the Exploitation Zone scripts to know which fine-tuned model to load.

CHECKPOINT_CONFIG_FILE = "active_checkpoint.json"


def get_checkpoints_dir():

   #  Get the checkpoints directory path.
    
    # IMPLEMENTATION NOTES
    # Uses __file__ when available (script execution) or falls back to os.getcwd()
    # (notebook execution). This ensures compatibility with both execution contexts,
    # which is important for interactive experimentation in Jupyter notebooks.
    
    # Returns:
    #     str: Absolute path to the checkpoints directory

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # Notebook environment: __file__ is not defined
        script_dir = os.getcwd()
    return os.path.join(script_dir, "../checkpoints")


def list_available_checkpoints() -> List[Dict]:

    L# ist all available checkpoints with their configurations.
    
    #METHODOLOGY

    # Scans the checkpoints directory for subdirectories containing valid
    # configuration files. Each checkpoint is validated by checking for:
    # 1. training_config.json (preferred, from our fine-tuning script)
    # 2. config.json (fallback, for external checkpoints)
    
    # This approach supports both internally-trained checkpoints and
    # potentially imported checkpoints from other sources.
    
    # Returns:
    #     List[Dict]: List of checkpoint information dictionaries containing:
    #         - name: Checkpoint directory name (used as identifier)
    #         - path: Absolute path to checkpoint directory
    #         - config: Parsed configuration dictionary

    checkpoints_dir = get_checkpoints_dir()
    checkpoints = []
    
    if not os.path.exists(checkpoints_dir):
        return checkpoints
    
    for item in os.listdir(checkpoints_dir):
        checkpoint_path = os.path.join(checkpoints_dir, item)
        if os.path.isdir(checkpoint_path):
            # Priority: training_config.json (our format) > config.json (generic)
            config_path = os.path.join(checkpoint_path, "training_config.json")
            if not os.path.exists(config_path):
                config_path = os.path.join(checkpoint_path, "config.json")
            
            if os.path.exists(config_path):
                try:
                    with open(config_path, "r") as f:
                        config = json.load(f)
                    checkpoints.append({
                        "name": item,
                        "path": checkpoint_path,
                        "config": config
                    })
                except Exception as e:
                    print(f" Warning: Could not read config for {item}: {e}")
    
    return checkpoints


def get_active_checkpoint() -> Optional[Dict]:
    
    # Get the currently active checkpoint configuration.
    
    # PURPOSE
    # The "active checkpoint" concept allows the Exploitation Zone to know
    # which fine-tuned model variant to load. When no checkpoint is active,
    # the system defaults to the baseline (pre-trained) CLIP model.
    
    # This supports the experimental design requirement (Constraint 9) of
    # comparing fine-tuned models against baselines.
    
    # Returns:
    #     Optional[Dict]: Active checkpoint configuration or None if:
    #         - No checkpoint is set
    #         - The configured checkpoint no longer exists

    checkpoints_dir = get_checkpoints_dir()
    config_file = os.path.join(checkpoints_dir, CHECKPOINT_CONFIG_FILE)
    
    if not os.path.exists(config_file):
        return None
    
    try:
        with open(config_file, "r") as f:
            active_config = json.load(f)
        
        # Validate checkpoint still exists (handles deleted checkpoints)
        checkpoint_path = active_config.get("checkpoint_path")
        if checkpoint_path and os.path.exists(checkpoint_path):
            return active_config
        else:
            print(f" Warning: Active checkpoint path does not exist: {checkpoint_path}")
            return None
    except Exception as e:
        print(f" Error reading active checkpoint config: {e}")
        return None


def set_active_checkpoint(checkpoint_name: str) -> bool:
    
    # Set a specific checkpoint as the active model for Exploitation Zone.
    
    # WORKFLOW
    
    # 1. Validates the checkpoint exists and is complete
    # 2. Persists the selection to active_checkpoint.json
    # 3. Returns success/failure status
    
    # This function is critical for A/B testing (Constraint 11), as it
    # determines which model variant is used for embedding generation
    # in downstream tasks.
    
    # Args:
    #     checkpoint_name: Name of the checkpoint directory to activate
        
    # Returns:
    #     bool: True if checkpoint was successfully activated, False otherwise

    checkpoints = list_available_checkpoints()
    
    # Find checkpoint by name
    checkpoint = None
    for cp in checkpoints:
        if cp["name"] == checkpoint_name:
            checkpoint = cp
            break
    
    if not checkpoint:
        print(f" Error: Checkpoint '{checkpoint_name}' not found")
        return False
    
    # Validate checkpoint integrity before activation
    if not _validate_checkpoint(checkpoint["path"]):
        print(f" Error: Checkpoint '{checkpoint_name}' is invalid or incomplete")
        return False
    
    # Persist active checkpoint configuration
    checkpoints_dir = get_checkpoints_dir()
    config_file = os.path.join(checkpoints_dir, CHECKPOINT_CONFIG_FILE)
    
    active_config = {
        "checkpoint_name": checkpoint_name,
        "checkpoint_path": checkpoint["path"],
        "config": checkpoint["config"],
        "activated_at": datetime.now().isoformat()  # Timestamp for audit trail
    }
    
    try:
        with open(config_file, "w") as f:
            json.dump(active_config, f, indent=2)
        print(f" Active checkpoint set to: {checkpoint_name}")
        return True
    except Exception as e:
        print(f" Error saving active checkpoint config: {e}")
        return False


def clear_active_checkpoint():

    # Clear the active checkpoint, reverting to baseline model.
    
    # EXPERIMENTAL DESIGN IMPACT

    # Clearing the active checkpoint causes the Exploitation Zone to use
    # the baseline (pre-trained) CLIP model. This is essential for:
    
    # 1. Establishing baseline performance metrics
    # 2. Conducting fair A/B comparisons
    # 3. Debugging fine-tuning issues by isolating variables
    
    # Per Constraint 9, the baseline acts as the control condition
    # against which fine-tuned variants are compared.
    checkpoints_dir = get_checkpoints_dir()
    config_file = os.path.join(checkpoints_dir, CHECKPOINT_CONFIG_FILE)
    
    if os.path.exists(config_file):
        os.remove(config_file)
        print(" Active checkpoint cleared. Using baseline model.")
    else:
        print(" No active checkpoint to clear.")


def _validate_checkpoint(checkpoint_path: str) -> bool:
    
    # Validate that a checkpoint is complete and usable.

    # VALIDATION CRITERIA
    
    # For LoRA/QLoRA fine-tuned CLIP models, a valid checkpoint must contain:
    
    # 1. Configuration file (training_config.json or config.json)
    #    - Records hyperparameters for reproducibility
    
    # 2. LoRA adapters for both encoders:
    #    - text_encoder/: Contains adapter_model.safetensors
    #    - vision_encoder/: Contains adapter_model.safetensors
    
    # The validation is intentionally lenient to support partial checkpoints
    # (e.g., interrupted training) while warning about potential issues.
    
    # Args:
    #     checkpoint_path: Absolute path to checkpoint directory
        
    # Returns:
    #     bool: True if checkpoint meets minimum validity requirements
    
    # Check for configuration files
    has_training_config = os.path.exists(os.path.join(checkpoint_path, "training_config.json"))
    has_config = os.path.exists(os.path.join(checkpoint_path, "config.json"))
    
    if not (has_training_config or has_config):
        return False
    
    # Check for LoRA adapters (PEFT library format)
    has_text_encoder = os.path.exists(os.path.join(checkpoint_path, "text_encoder"))
    has_vision_encoder = os.path.exists(os.path.join(checkpoint_path, "vision_encoder"))
    
    # For CLIP with LoRA, both encoders should have adapters
    if has_text_encoder and has_vision_encoder:
        return True
    
    # Fallback: check for legacy model state format
    has_model_state = os.path.exists(os.path.join(checkpoint_path, "model_state.pt"))
    if has_model_state:
        return True
    
    # Allow partial checkpoints with config only (with implicit warning)
    return has_training_config or has_config


def get_best_checkpoint() -> Optional[str]:
    
    # Automatically select the checkpoint with the lowest training loss.

    
    checkpoints = list_available_checkpoints()
    
    if not checkpoints:
        return None
    
    best_checkpoint = None
    best_loss = float('inf')
    
    for cp in checkpoints:
        config = cp.get('config', {})
        training_history = config.get('training_history', [])
        
        if training_history:
            # Use final epoch loss as the selection criterion
            final_loss = training_history[-1].get('loss')
            if isinstance(final_loss, (int, float)) and final_loss < best_loss:
                best_loss = final_loss
                best_checkpoint = cp['name']
    
    return best_checkpoint


def display_checkpoints():
    
   # Display a formatted summary of all available checkpoints.
    
    # OUTPUT FORMAT
    
    # For each checkpoint, displays:
    # - Name and activation status
    # - Base model and checkpoint identifier
    # - Fine-tuning method (LoRA vs QLoRA)
    # - LoRA hyperparameters (r, alpha)
    # - Final training loss (if available)
    # - Creation timestamp
    
    # This supports Constraint 11 (Reporting and Visualization) by providing
    # a clear overview of the experimental landscape.
    
    print("\n" + "=" * 60)
    print(" Available Checkpoints")
    print("=" * 60)
    
    checkpoints = list_available_checkpoints()
    active_checkpoint = get_active_checkpoint()
    
    if not checkpoints:
        print(" No checkpoints found.")
        return
    
    for i, cp in enumerate(checkpoints, 1):
        is_active = active_checkpoint and cp["name"] == active_checkpoint.get("checkpoint_name")
        status = " [ACTIVE]" if is_active else ""
        
        config = cp["config"]
        print(f"\n {i}. {cp['name']}{status}")
        print(f"    Model: {config.get('model_name', 'N/A')}/{config.get('checkpoint', 'N/A')}")
        print(f"    Method: {'QLoRA' if config.get('use_qlora') else 'LoRA'}")
        print(f"    LoRA r: {config.get('lora_r', 'N/A')}, alpha: {config.get('lora_alpha', 'N/A')}")
        if "training_history" in config and config["training_history"]:
            final_loss = config["training_history"][-1].get("loss", "N/A")
            print(f"    Final loss: {final_loss:.4f}" if isinstance(final_loss, (int, float)) else f"    Final loss: {final_loss}")
        print(f"    Created: {config.get('created_at', 'N/A')}")
    
    if not active_checkpoint:
        print("\n No active checkpoint set. Using baseline model.")
    else:
        print(f"\n Active checkpoint: {active_checkpoint.get('checkpoint_name')}")


# ==============================================================================
# INTERACTIVE MODE
# ==============================================================================
# Provides a user-friendly interface for checkpoint management operations.
# This supports iterative experimentation workflows common in ML research.

def run_interactive_mode():
    # Run interactive checkpoint management mode.

    
    print("\n" + "=" * 60)
    print(" Checkpoint Manager - Interactive Mode")
    print("=" * 60)
    
    while True:
        print("\n Available Commands:")
        print("  1. list   - List all available checkpoints")
        print("  2. set    - Set active checkpoint")
        print("  3. auto   - Auto-select best checkpoint (lowest loss)")
        print("  4. clear  - Clear active checkpoint (use baseline)")
        print("  5. get    - Get active checkpoint info")
        print("  6. exit   - Exit checkpoint manager")
        print("-" * 60)
        
        try:
            command = input("\n Enter command (1-6 or name): ").strip().lower()
            
            if command == "exit" or command == "6":
                print(" Exiting checkpoint manager...")
                break
            
            elif command == "list" or command == "1":
                display_checkpoints()
            
            elif command == "set" or command == "2":
                # Show available checkpoints for user selection
                checkpoints = list_available_checkpoints()
                if not checkpoints:
                    print(" No checkpoints available.")
                    continue
                
                print("\n Available checkpoints:")
                for i, cp in enumerate(checkpoints, 1):
                    config = cp.get('config', {})
                    training_history = config.get('training_history', [])
                    loss_info = ""
                    if training_history:
                        final_loss = training_history[-1].get('loss')
                        if isinstance(final_loss, (int, float)):
                            loss_info = f" (loss: {final_loss:.4f})"
                    print(f"  {i}. {cp['name']}{loss_info}")
                
                choice = input("\n Enter checkpoint number or name: ").strip()
                
                # Support both numeric and name-based selection
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(checkpoints):
                        checkpoint_name = checkpoints[idx]['name']
                    else:
                        print(" Invalid checkpoint number.")
                        continue
                except ValueError:
                    checkpoint_name = choice
                
                set_active_checkpoint(checkpoint_name)
            
            elif command == "auto" or command == "3":
                print("\n Auto-selecting best checkpoint (lowest training loss)...")
                best_checkpoint = get_best_checkpoint()
                
                if best_checkpoint:
                    print(f" Best checkpoint found: {best_checkpoint}")
                    # Display loss for confirmation
                    checkpoints = list_available_checkpoints()
                    for cp in checkpoints:
                        if cp['name'] == best_checkpoint:
                            config = cp.get('config', {})
                            training_history = config.get('training_history', [])
                            if training_history:
                                final_loss = training_history[-1].get('loss')
                                if isinstance(final_loss, (int, float)):
                                    print(f" Training loss: {final_loss:.4f}")
                            break
                    
                    confirm = input(f"\n Set '{best_checkpoint}' as active checkpoint? (y/n): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        set_active_checkpoint(best_checkpoint)
                    else:
                        print(" Cancelled.")
                else:
                    print(" No checkpoints with training history found.")
            
            elif command == "clear" or command == "4":
                clear_active_checkpoint()
            
            elif command == "get" or command == "5":
                active = get_active_checkpoint()
                if active:
                    print("\n Active Checkpoint:")
                    print(json.dumps(active, indent=2))
                else:
                    print("\n No active checkpoint set. Using baseline model.")
            
            else:
                print(f" Unknown command: {command}")
                print(" Please enter a valid command (list, set, auto, clear, get, exit)")
        
        except KeyboardInterrupt:
            print("\n\n Exiting checkpoint manager...")
            break
        except Exception as e:
            print(f" Error: {e}")


# Convenience function for programmatic use
def manage():
    run_interactive_mode()


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    # Always run in interactive mode when executed directly
    run_interactive_mode()
