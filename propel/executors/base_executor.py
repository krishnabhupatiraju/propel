class BaseExecutor(object):
    
    def __init__(self):
        raise NotImplementedError('Abstract Executor. Call a concrete'
                                  ' implementation instead')
    
    def execute(self, command, *args, **kwargs):
        """
        Abstract method to execute "command" through the execution 
        platform
        """
        raise NotImplementedError('Abstract Executor. Call a concrete'
                                  ' implementation instead')
        