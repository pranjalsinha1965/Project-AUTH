from datetime import datetime
from typing import Callable, Generator, Any, Dict, Tuple

def pipeline() -> Tuple[Callable[[str], Callable[[Callable], Callable]], Callable[[Any], Generator[bool, None, None]]]:
    """
    Initializes a pipeline with steps stored in a dictionary, 
    where each step is a generator function that yields intermediate results.
    
    Returns:
        add_step (Callable): Decorator to add steps to the pipeline.
        execute (Callable): Function to execute the pipeline as a generator.
    """
    steps: Dict[str, Callable] = {}

    def add_step(name: str) -> Callable[[Callable], Callable]:
        """
        Decorator to add a step to the pipeline.
        
        Args:
            name (str): The name of the step.
        
        Returns:
            decorator (Callable): A decorator that registers the step function.
        """
        def decorator(func: Callable) -> Callable:
            steps[name] = func
            return func
        return decorator

    def execute(data: Any) -> Generator[bool, None, None]:
        """
        Executes the pipeline sequentially as a generator.
        
        Args:
            data (Any): The initial data to be processed by the pipeline steps.
        
        Yields:
            bool: True if step completes successfully; False if a step fails.
        """
        result = data
        for step_name, step_func in steps.items():
            result = step_func(result)
            if result is None:
                yield False  # Stop the pipeline if any step returns None
                return
            yield True

    return add_step, execute

# Initialize pipeline
add_step, execute_pipeline = pipeline()

# Audit log to store operations
audit_log = []

# Dictionary for data storage
data_store = {
    "100": {"name": "John Doe", "age": 30, "department": "HR"},
    "101": {"name": "Jane Smith", "age": 28, "department": "Engineering"}
}

# Role-based access control
roles = {
    "host": {"can_delete": False, "can_get": True},
    "hr": {"can_delete": True, "can_get": True}
}

def log_action(user_id: str, action: str) -> None:
    """
    Logs an action performed by a user with a timestamp.
    
    Args:
        user_id (str): ID of the user performing the action.
        action (str): Description of the action performed.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    audit_log.append(f"{timestamp} - User {user_id} performed action: {action}")

@add_step("validation")
def validate_user(data: Dict[str, Any]) -> Any:
    """
    Validates the username and password.
    
    Args:
        data (Dict[str, Any]): Dictionary containing user data including ID, username, and password.
    
    Returns:
        Any: The original data if validation succeeds; None if validation fails.
    """
    user_id, username, password = data["user_id"], data["username"], data["password"]
    if len(username) < 3 or len(password) < 6:
        print("Validation failed for username or password.")
        return None
    log_action(user_id, "Validation successful")
    return data

@add_step("get_data")
def get_data(data: Dict[str, Any]) -> Any:
    """
    Retrieves user data if the role has the necessary permissions.
    
    Args:
        data (Dict[str, Any]): Dictionary containing user data including role and target ID.
    
    Returns:
        Any: The original data if retrieval is successful; None if access is denied.
    """
    user_id, target_id = data["user_id"], data["target_id"]
    role = roles.get(data["role"], {})
    
    if not role.get("can_get", False):
        print("Access denied: You do not have permission to view data.")
        return None
    
    record = data_store.get(target_id)
    if record:
        log_action(user_id, f"Viewed record for user {target_id}")
        print(f"Data for user {target_id}: {record}")
    else:
        print("No record found.")
    return data

@add_step("delete_data")
def delete_data(data: Dict[str, Any]) -> Any:
    """
    Deletes non-essential data if the role has the necessary permissions, keeping essential fields intact.
    
    Args:
        data (Dict[str, Any]): Dictionary containing user data including role and target ID.
    
    Returns:
        Any: The original data if deletion is successful; None if access is denied.
    """
    user_id, target_id = data["user_id"], data["target_id"]
    role = roles.get(data["role"], {})

    if not role.get("can_delete", False):
        print("Access denied: You do not have permission to delete data.")
        return None

    record = data_store.get(target_id)
    if record:
        # Preserve essential fields only
        data_store[target_id] = {key: record[key] for key in ["name", "age", "department"]}
        log_action(user_id, f"Deleted non-essential data for user {target_id}")
        print(f"Non-essential data for user {target_id} deleted.")
    else:
        print("No record found to delete.")
    return data

# Example execution with user input and role-based access
host_data = {"user_id": "102", "role": "host", "target_id": "100", "username": "host_user", "password": "hostpass123"}
hr_data = {"user_id": "103", "role": "hr", "target_id": "100", "username": "hr_user", "password": "hrpass123"}

print("Executing pipeline as Host:")
for result in execute_pipeline(host_data):
    print("Pipeline step result:", result)

print("\nAudit log:", audit_log)

print("\nExecuting pipeline as HR:")
for result in execute_pipeline(hr_data):
    print("Pipeline step result:", result)

print("\nAudit log:", audit_log)
