# Utility for consistent progress bars across scripts
from tqdm import tqdm
from typing import Iterable, Callable, Optional

def progress_bar_iter(iterable: Iterable, total: Optional[int] = None, desc: str = "", get_desc: Optional[Callable] = None):
    """
    Wrap an iterable with a tqdm progress bar, optionally updating the description with get_desc(item).
    Args:
        iterable: The iterable to wrap
        total: Total number of items (if known)
        desc: Static description (e.g., table name)
        get_desc: Function to get dynamic description from current item
    Returns:
        A generator-like object that yields items from the iterable and exposes the bar attribute
    """
    class ProgressBarIterator:
        def __init__(self, iterable, total, desc, get_desc):
            self.iterable = iterable
            self.total = total
            self.desc = desc
            self.get_desc = get_desc
            self.bar = None
            
        def __iter__(self):
            # Use a wider progress bar (100 columns) and show the bar position
            self.bar = tqdm(self.iterable, total=self.total, desc=self.desc, ncols=100, 
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
            for item in self.bar:
                if self.get_desc:
                    self.bar.set_description(f"{self.desc}: {self.get_desc(item)}")
                yield item
            self.bar.close()
    
    return ProgressBarIterator(iterable, total, desc, get_desc)
