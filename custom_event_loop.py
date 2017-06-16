import types


@types.coroutine
def g(i):
    message = yield
    print('g{}: {}'.format(i, message))
    return message


async def f(i):
    while True:
        message = await g(i)
        print('f{}: {}'.format(i, message))

        if message == 'break':
            print('f{}: breaking...'.format(i))
            break


def run():
    queue = [f(0), f(1), f(2)]
    for future in queue:
        future.send(None)

    while True:
        message = input()
        for i, future in enumerate(queue):
            if future:
                try:
                    print('run: {}'.format(i))
                    future.send(message)
                except StopIteration:
                    queue[i] = None
