#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   weighted_random.py
#
#   Description     :   Return a set of values based on input set and weigthings
#
#   Created     	:   06 Aug 2025
#
#   Descriptin      :   A class for weighted random selection with multiple implementation methods.
#
#                       You can implement weighted random selection in Python using several approaches. 
#                       Here are the most common and effective methods:Weighted Random Selection MethodsCode import random
#
#   Method 1: random.choices() (Recommended)
#       This is the simplest and most efficient approach for Python 3.6+:
#       pythonrandom.choices(names, weights=weights, k=1)[0]
#
#   Method 2: numpy.random.choice()
#       If you're already using NumPy:
#       pythonnp.random.choice(names, p=probabilities)
#
#   Method 3: Manual Implementation
#       A custom implementation using cumulative weights - useful for understanding the underlying logic.
#       Key Features:
#
#   WeightedRandomSelector class: Reusable class that supports all three methods
#       Validation: Checks if weights sum to the expected scale
#       Testing: Includes distribution testing to verify the implementation works correctly
#       Simple function: weighted_random_simple() for quick one-off use
#
#   Usage:
#       python# Your color example
#       options = [
#           {"name": "orange", "value": 0.3},
#           {"name": "blue", "value": 0.25},
#       # ... etc
#       ]
#
#   selector     = WeightedRandomSelector(options)
#   random_color = selector.get_random()  # 30% chance of "orange"
#
#   The random.choices() method is generally the best choice as it's built into Python's standard library, is well-optimized, 
#   and handles edge cases properly. The weights don't need to sum to 1.0 - they can be any positive numbers and will be 
#   automatically normalized.
#
#   Functions       :   WeightedRandomSelector (Class)
#                   :       __init__
#                   :       method1_random_choices
#                   :       method2_numpy_choice
#                   :       method3_manual_cumulative
#                   :       get_random
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


import random
import numpy as np
from typing import List, Dict, Any, Union


class WeightedRandomSelector:
    
    
    def __init__(self, options: List[Dict[str, Union[str, float, Dict]]], scale: float = 1.0):
        
        """
        Initialize with options and scale.
        
        Args:
            options: List of dicts with 'name' and 'value' keys
                'name' can be a string or a document/dict object
            scale: Total weight scale (should match sum of all values)
        """
        
        self.options = options
        self.scale   = scale
        self.names   = [option['name'] for option in options]  # Can be strings or dicts
        self.weights = [option['value'] for option in options]
        
        # Validate weights sum to scale
        total_weight = sum(self.weights)
        if abs(total_weight - scale) > 1e-10:
            print(f"Warning: Weights sum to {total_weight}, expected {scale}")

        #end if
    #end def
    
    def method1_random_choices(self) -> Union[str, Dict]:
        
        """Method 1: Using random.choices() - Python 3.6+"""
        
        return random.choices(self.names, weights=self.weights, k=1)[0]
        #end def
    
    def method2_numpy_choice(self) -> Union[str, Dict]:
        
        """Method 2: Using numpy.random.choice() - Note: Only works with strings"""
        if any(isinstance(name, dict) for name in self.names):
            np.choice #doesn't work with dict objects, fall back to manual method
            
            return self.method3_manual_cumulative()
        
        #end if
        
        # Normalize weights to probabilities
        probabilities = np.array(self.weights) / sum(self.weights)
        
        return np.random.choice(self.names, p=probabilities)
    #end def
    
    
    def method3_manual_cumulative(self) -> Union[str, Dict]:
        
        """Method 3: Manual implementation using cumulative weights"""
        
        # Create cumulative weights
        cumulative_weights = []
        cumsum = 0
        for weight in self.weights:
            cumsum += weight
            cumulative_weights.append(cumsum)
        
        #end for
        
        # Generate random number and find corresponding item
        rand_val = random.random() * cumulative_weights[-1]
        
        for i, cum_weight in enumerate(cumulative_weights):
            if rand_val <= cum_weight:
                return self.names[i]
            
            #end if
        #end for
        
        # Fallback (shouldn't reach here)
        return self.names[-1]
    # end def
    
    
    def get_random(self, method: str = "choices") -> Union[str, Dict]:
        
        """
        Get a weighted random selection using specified method.
        
        Args:
            method: 'choices', 'numpy', or 'manual'
        
        Returns:
            The selected 'name' value (string or dict/document)
        """
        
        if method == "choices":
            return self.method1_random_choices()
        
        elif method == "numpy":
            return self.method2_numpy_choice()
        
        elif method == "manual":
            return self.method3_manual_cumulative()
        
        else:
            raise ValueError("Method must be 'choices', 'numpy', or 'manual'")

        #end if
    #end def
#end class


# # Example usage and testing
# if __name__ == "__main__":
    
#     # Example 1: Colors (strings)
#     color_options = [
#         {"name": "orange", "value": 0.6},
#         {"name": "blue", "value":   0.5},
#         {"name": "green", "value":  0.20},
#         {"name": "yellow", "value": 0.04},
#         {"name": "red", "value":    0.02},
#         {"name": "grey", "value":   0.04},
#         {"name": "black", "value":  0.20},
#         {"name": "white", "value":  0.40}
#     ]
        
#     # Example 2: Document objects as names
#     document_options = [
#         {
#             "name": {"id": 1, "title": "Python Guide", "category": "programming", "pages": 150},
#             "value": 0.4
#         },
#         {
#             "name": {"id": 2, "title": "Data Science Handbook", "category": "data", "pages": 300},
#             "value": 0.15
#         },
#         {
#             "name": {"id": 3, "title": "Web Development", "category": "web", "pages": 200},
#             "value": 0.15
#         },
#         {
#             "name": {"id": 4, "title": "UI Development", "category": "web", "pages": 100},
#             "value": 0.1
#         },
#         {
#             "name": {"id": 5, "title": "DB Development", "category": "web", "pages": 50},
#             "value": 0.2
#         }    
#     ]
    
#     # Example 3: Mixed types (though not recommended)
#     mixed_options = [
#         {"name": "simple_string", "value": 0.3},
#         {"name": {"type": "complex", "data": [1, 2, 3]}, "value": 0.7}
#     ]
    
    
    
#     # Create selectors
#     color_selector    = WeightedRandomSelector(color_options,    scale=2.0)
#     document_selector = WeightedRandomSelector(document_options, scale=1.0)
#     mixed_selector    = WeightedRandomSelector(mixed_options,    scale=1.0)
    
#     print("=== String Names (Colors) ===")
#     for i in range(5):
#         result = color_selector.get_random()
#         print(f"Selection {i+1}: {result} (type: {type(result).__name__})")
    
    
#     print("\n=== Document Objects as Names ===")
#     for i in range(7):
#         result = document_selector.get_random()
#         print(f"Selection {i+1}: {result}")
#         print(f"  Title: {result['title']}")
#         print(f"  Type: {type(result).__name__}")
#         print()
    
    
#     print("=== Mixed Types ===")
#     for i in range(3):
#         result = mixed_selector.get_random()
#         print(f"Selection {i+1}: {result} (type: {type(result).__name__})")
    
    
#     print("\n=== Testing Distribution with Documents (100 samples) ===")
#     doc_counts = {}
#     for _ in range(100):
#         doc    = document_selector.get_random()
#         title  = doc["title"]
#         doc_counts[title] = doc_counts.get(title, 0) + 1
    
    
#     print("Document distribution:")
#     for option in document_options:
#         title    = option["name"]["title"]
#         expected = option["value"] * 100
#         actual   = (doc_counts.get(title, 0) / 100) * 100
#         print(f"  {title:20}: {actual:5.1f}% (expected {expected:5.1f}%)")



# # Alternative: Simple function-based approach that works with any object type
# def weighted_random_simple(options: List[Dict[str, Union[str, float, Dict, Any]]]) -> Any:
#     """
#     Simple function for weighted random selection that works with any object type.
    
#     Args:
#         options: List of dicts with 'name' and 'value' keys
#                 'name' can be any type (string, dict, list, object, etc.)
    
#     Returns:
#         Randomly selected name based on weights (any type)
#     """
#     names   = [option['name'] for option in options]
#     weights = [option['value'] for option in options]
    
#     return random.choices(names, weights=weights, k=1)[0]


# # Usage examples for the simple function with documents
# print("\n=== Simple Function with Document Objects ===")
# docs = [
#     {"name": {"id": "doc1", "content": "Hello World"},      "value": 0.3},
#     {"name": {"id": "doc2", "content": "Goodbye World"},    "value": 0.2},
#     {"name": {"id": "doc3", "content": "Good Night World"}, "value": 0.1},
#     {"name": {"id": "doc4", "content": "Watch out World"},  "value": 0.3}
# ]

# for i in range(10):
#     selected_doc = weighted_random_simple(docs)
#     print(f"Selection {i+1}: {selected_doc}")