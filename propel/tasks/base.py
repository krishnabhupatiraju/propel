class BaseTask(object):
    """
    Abstract BaseTask class that concrete subclasses will
    implement
    """

    def get(self, user_id, *args, **kwargs):
        """ 
        Method to capture objects from platform E.g. tweets 
        
        :param user_id: id of the user
        :type user_id: string        
        """
        raise NotImplementedError()

    def query(self, user_id, from_ts, to_ts, *args, **kwargs):
        """ 
        Get objects saved in the local database 
        
        :param user_id: id of the user
        :type user_id: string
        :param from_ts: Objects created from this epoch
        :type from_ts: int
        :param to_ts: Objects created until this epoch
        :type to_ts: int        
        """
        raise NotImplementedError()
    
    def schedule_args(self):
        """
        Returns the args using which the scheduler calls the 
        get method
        """
        raise NotImplementedError()