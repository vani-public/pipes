from __future__ import print_function

from pypipes.message import Message, FrozenMessage, MessageUpdate

# pipeline message is typically a dictionary
# Message class allows access message items like object attributes
msg = Message()
# lets add some keys
msg.key1 = 'value1'
msg.key2 = 'value2'

print('Message object:', msg)

# update and delete keys
msg.key1 = 'updated value1'
del msg.key2

print('Updated message:', msg)

# FrozenMessage is expected to be used where message updating is not desirable.
# So it blocks common dictionary update operations but not all of them

frozen_msg = FrozenMessage(key1='original value')

try:
    frozen_msg['key1'] = 1
except AttributeError:
    print('Set item is blocked')

try:
    frozen_msg.key1 = 1
except AttributeError:
    print('Set attribute is blocked')

try:
    del frozen_msg['key1']
except AttributeError:
    print('Deleting of item is blocked')

try:
    del frozen_msg.key1
except AttributeError:
    print('Deleting of attribute is blocked')

try:
    frozen_msg.update(key1=1)
except AttributeError:
    print('Update is blocked')

print('Frozen message is not changed:', frozen_msg)


# MessageUpdate is a subclass of a Message that expected to be used for saving a message update
# this update can be merged with some other message later

msg = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
msg_update = MessageUpdate()
print('Update is empty:', bool(MessageUpdate()))
del msg_update.key1
print('After a delete operation this update is not empty:', bool(msg_update))

msg_update.key2 = 'updated value2'
msg_update.key4 = 'added value4'

print('Update is:', msg_update)
print('Message merge result is:', msg_update.merge_with_message(msg))
