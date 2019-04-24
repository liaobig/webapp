import asyncio, logging
import aiomysql

async def create_pool(loop, **kw):#建连接池
    
    '''loop:is an optional event loop instance, asyncio.get_event_loop() is used
    if loop is not specified.'''
    global __pool
    logging.info('create database connection pool...')


    '''dict有一个get方法，如果dict中有对应的value值，则返回对应于key的value
    值，否则返回默认值'''
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),#默认自动提交事务，不用手动去提交事务
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop #loop默认是asyncio.get_event_loop()的loop
)
    
def log(sql, args=()):
    logging.info('SQL: %s' % sql)
    
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    
    async with __pool.get() as conn:
    #.get()方法在创建了连接池的情况下使用
    #连接上，然后as为conn
        
        async with conn.cursor(aiomysql.DictCursor) as cur:
        #aiomysql.DictCursor返回的游标为一个字典
        #使用conn创建游标cursor ,as为cur
            
            await cur.execute(sql.replace('?', '%s'), args or ())
            #游标执行占位符转换 ‘？’ → ‘%s’
            if size:
                rs = await cur.fetchmany(size)#如果传入size参数，就通过fetchmany()获取最多指定数量的记录
            else:
                rs = await cur.fetchall()#一次性返回所有的查询结果
        logging.info('rows returned: %s' % len(rs))
        return rs
    
async def execute(sql, args, autocommit=True):
#execute()函数和select()函数所不同的是，cursor对象不返回结果集，而是通过rowcount返回结果数。
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount#获取影响的行数
            if not autocommit:
                await conn.commit()#提交事务
        except BaseException as e:
            if not autocommit:
                await conn.rollback()#回滚到事务启动前的状态
            raise
        return affected
    
def create_args_string(num):
    lol=[]
    for n in range(num):
        lol.append('?')
    return (','.join(lol))


# 定义Field类，负责保存(数据库)表的字段名和字段类型
class Field(object):
    # 表的字段包含名字、类型、是否为表的主键和默认值
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default
    def __str__(self):
        # 返回 表名字 字段名 和字段类型
        return "<%s , %s , %s>" %(self.__class__.__name__, self.name, self.column_type)

# 定义数据库中五个存储类型
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name,ddl,primary_key,default)
        
# 布尔类型不可以作为主键
class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name,'Boolean',False, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)
        
class FloatField(Field):
    def __init__(self, name=None, primary_key=False,default=0.0):
        super().__init__(name, 'real', primary_key, default)
        
class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name,'text',False, default)

class ModelMetaclass(type):
#按照默认习惯，metaclass的类名总是以Metaclass结尾，metaclass是创建类，所以必须从`type`类型派生
    
    # cls:当前准备创建的类的对象
    # name:类的名字,创建User类，则name便是User
    # bases：代表继承父类的集合,创建User类，则base便是Model
    # attrs：类的方法集合，创建User类，则attrs便是一个包含User类属性的dict
    def __new__(cls, name, bases, attrs):
        
        # # 因为Model类是基类，所以排除掉,因为要排除对model类的修改
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        
        # 取出表名，默认与类的名字相同 
        table_name=attrs.get('__table__', None) or name   #如果存在表名，则返回表名，否则返回 name
        logging.info('found table: %s (table: %s) ' %(name,table_name ))

        mappings=dict()# 用于存储所有的字段，以及字段值
        fields=[]  #field保存的是除主键外的属性名
        primaryKey=None
        
        # 注意这里attrs的key是字段名，value是字段实例，不是字段的具体值
        # 比如User类的id=StringField(...) 这个value就是这个StringField的一个实例，而不是实例化
        # 的时候传进去的具体id值
        for k, v in attrs.items():
            if isinstance(v, Field):#判断v的变量类型是否为Field
            # attrs同时还会拿到一些其它系统提供的类属性，我们只处理自定义的类属性，所以判断一下
            # isinstance 方法用于判断v是否是一个Field
            
                logging.info('Found mapping %s===>%s' %(k, v))
                mappings[k] = v
                
                if v.primary_key:
                    logging.info('fond primary key %s'%k)
                    # 这里很有意思 当第一次主键存在primaryKey被赋值 后来如果再出现主键的话就会引发错误
                    if primaryKey:
                        raise RuntimeError('Duplicated key for field') #一个表只能有一个主键，当再出现一个主键的时候就报错
                    primaryKey=k # 也就是说主键只能被设置一次
                else:
                    fields.append(k)
 
        if not primaryKey:  #如果主键不存在也将会报错，在这个表中没有找到主键，一个表只能有一个主键，而且必须有一个主键
            raise RuntimeError('Primary key not found!')
        
        # 这里的目的是去除类属性，为什么要去除呢，因为我想知道的信息已经记录下来了。
        # 去除之后，就访问不到类属性了
        # 记录到了mappings,fields，等变量里，而我们实例化的时候，如
        # user=User(id='10001') ，为了防止这个实例变量与类属性冲突，所以将其去掉
        for k in mappings.keys():
            attrs.pop(k)
            
        # 保存除主键外的属性为''列表形式
        # 将除主键外的其他属性变成`id`, `name`这种形式，关于反引号``的用法，可以参考点击打开链接
        escaped_fields=list(map(lambda f:'`%s`' %f, fields))

        # 以下都是要返回的东西了，刚刚记录下的东西，如果不返回给这个类，又谈得上什么动态创建呢？
        # 到此，动态创建便比较清晰了，各个子类根据自己的字段名不同，动态创建了自己
        # 下面通过attrs返回的东西，在子类里都能通过实例拿到，如self

        # 保存属性和列的映射关系
        attrs['__mappings__']=mappings
        # 保存表名
        attrs['__table__']=table_name  #这里的tablename并没有转换成反引号的形式
        # 保存主键名称
        attrs['__primary_key__']=primaryKey
        # 保存主键外的属性名
        attrs['__fields__']=fields
        
        # 只是为了Model编写方便，放在元类里和放在Model里都可以
        # 构造默认的增删改查 语句
        attrs['__select__']='select `%s`, %s from `%s` '%(primaryKey,', '.join(escaped_fields), table_name)
        attrs['__insert__'] = 'insert into  `%s` (%s, `%s`) values (%s) ' %(table_name, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields)+1))
        attrs['__update__']='update `%s` set %s where `%s` = ?' % (table_name, ', '.join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__']='delete from `%s` where `%s`=?' %(table_name, primaryKey)
        return type.__new__(cls, name, bases, attrs)
    
        #class User(Model):
            # 定义类的属性到列的映射：
            #id = IntegerField('id')
            #name = StringField('username')
            #email = StringField('email')
            #password = StringField('password')
        #u = User(id=12345, name='Michael', email='test@orm.org', password='my-pwd')
        #u.__fields__直接调用主键外的属性名
 
# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看作是对所有数据库表操作的基本定义的映射
 
 
# 基于字典查询形式
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作
# 实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法

class Model(dict, metaclass=ModelMetaclass):
#Python解释器在创建MModel时，要通过ModelMetaclass.__new__()来创建
    def __init__(self, **kw):
        super().__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    # 类方法有类变量cls传入，从而可以用cls做一些相关的处理。
    # 并且有子类继承时，调用该类方法时，传入的类变量cls是子类，而非父类
    async def findAll(cls, where=None, args=None, **kw):# cls : 表示没用被实例化的类本身
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])#返回一条记录，以dict的形式返回，因为cls的夫类继承了dict类
    #User类现在就可以通过类方法实现主键查找：
    #user = yield from User.find('123')

    async def save(self):
    #调用时需要特别注意：user.save()没有任何效果，因为调用save()仅仅是创建了一个协程，并没有执行它。一定要用：await
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)
    #这样，就可以把一个User实例存入数据库：
    #user = User(id=123, name='Michael')
    #yield from user.save()
            
    async def update(self):
    
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)
