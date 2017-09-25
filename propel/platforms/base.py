class BasePlatform(object):
    """
    Abstract Base Platform class that concrete subclasses will 
    implement
    """

    def capture_objects(self, user_id, *args, **kwargs):
        """ Method to capture objects from platform E.g. tweets 
        
        :param user_id: id of the user
        :type user_id: string        
        """
        raise NotImplementedError()

    def query_objects(self, user_id, from_ts, to_ts, *args, **kwargs):
        """ Get objects saved in the local database 
        
        :param user_id: id of the user
        :type user_id: string
        :param from_ts: Objects created from this epoch
        :type from_ts: int
        :param to_ts: Objects created until this epoch
        :type to_ts: int        
        """
        raise NotImplementedError()     