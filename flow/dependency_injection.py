from functools import wraps

class Registry(object):
    
    _r = dict()
    
    @classmethod
    def register(cls, for_class,impl):
        cls._r[for_class.__name__] = impl
    
    @classmethod
    def get_instance(cls, for_class, *args, **kwargs):
        satisfies = cls._r[for_class.__name__]
        if callable(satisfies):
            return satisfies(*args,**kwargs)
        return satisfies
    
    @classmethod
    def clear(cls):
        cls._r = dict()
    
def dependency(cls):
    
    def x(fn):     
        @wraps(fn)
        def y(inst,*args,**kwargs):      
            try:
                o = inst._registry[cls.__name__]
                if callable(o):
                    o = o(*args,**kwargs)
                setattr(o,'_registry',inst._registry)
                return o
            except AttributeError:
                return Registry.get_instance(cls,*args,**kwargs)
            except KeyError:
                o = Registry.get_instance(cls,*args,**kwargs)
                setattr(o,'_registry',inst._registry)
                return o
        return y
    
    return x