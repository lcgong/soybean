class InvalidTopicName(Exception):
    ...

class InvalidGroupId(Exception):
    ...

class SendingError(Exception):
    ...

class UnkownArgumentError(ValueError):
    pass

class ActionError(Exception):
    ...

class TrasnactionPreparingError(ActionError):
    ...

