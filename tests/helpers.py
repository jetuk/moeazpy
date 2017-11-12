


def assert_multipart_message_call_args(func, sender, expected_message):
    frames = func.call_args[0][0]
    assert frames[0] == sender
    assert frames[1] == b''
    assert frames[2] == expected_message
    # Assert no kwargs given
    assert func.call_args[1] == {}

def assert_message_call_args(func, expected_message):
    print(func.call_args[0][0])
    assert func.call_args[0][0] == expected_message
    assert func.call_args[1] == {}