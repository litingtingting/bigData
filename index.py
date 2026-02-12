#scripts enter
import sys
import traceback

#import example.enter

class LazyImport(object):
    """
    动态导入模块
    """
    def __init__(self, module_name, module_class):
        """
        :param module_name:
        :param module_class:
        :return: 等同于 form module_name import module_class
        """
        self.module_name = module_name
        self.module_class = module_class
        self.module = None
 
    def __getattr__(self, name):
        if self.module is None:
            self.module = __import__(self.module_name, fromlist=[self.module_class])
        return getattr(self.module, name)


if __name__ == "__main__":

	if len(sys.argv) < 3:
		print("Usage: index.py <module>  <action>  <param1=value1> ...", file=sys.stderr)
		sys.exit(-1)

	module = 'controllers.' + sys.argv[1]
	action = sys.argv[2]
	#clsname = module.split('.').pop()
	clsname = sys.argv[1]

	data = {}
	if len(sys.argv) > 3:
		temp = sys.argv[3:]
		for i in temp:
			i = i.strip().split('=')
			data[i[0]] = i[1]
	try:
		importmodule = LazyImport(module, clsname)
		classname = getattr(importmodule, clsname)
		func = getattr(classname, action)
		func (data)
	except Exception as e:
		print('Error:\t', str(e))
		print('Trace.\n%s' % traceback.format_exc())