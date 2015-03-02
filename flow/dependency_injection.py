from functools import wraps

class Registry(object):
    
    instance = None
    
    def __init__(self):
        super(Registry,self).__init__()
        self._r = dict()
    
    def _register(self,for_class,impl):
        self._r[for_class.__name__] = impl
    
    @classmethod
    def register(cls,for_class,impl):
        cls.instance._register(for_class,impl)
    
    def _get_instance(self,for_class,*args,**kwargs):
        satisfies = self._r[for_class.__name__]
        if callable(satisfies):
            return satisfies(*args,**kwargs)
        return satisfies
    
    @classmethod
    def get_instance(cls,for_class,*args,**kwargs):
        return cls.instance._get_instance(for_class,*args,**kwargs)
    
    def _clear(self):
        self._r = dict()
    
    @classmethod
    def clear(cls):
        cls.instance._clear()

Registry.instance = Registry()

def dependency(cls):
    
    def x(fn):
        
        @wraps(fn)
        def y(inst,*args,**kwargs):      
            try:
                o = inst._registry._get_instance(cls,*args,**kwargs)
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

def _register(x,for_cls,impl):
    if not hasattr(x,'_registry'):
        setattr(x,'_registry',Registry())
        try:
            x._on_register()
        except AttributeError:
            pass
    x._registry._register(for_cls,impl)

def register(*args):
    if len(args) == 2:
        def x(cls):
            _register(cls,*args)
            return cls
        return x
    
    _register(*args)
    return args[0]