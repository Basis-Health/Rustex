# Rustex

Context Aware Async Mutex

## Example
```python
import asyncio
import random
import contextvars
import uuid

import rustex

mutex = rustex.Rustex()
cv = contextvars.ContextVar('rustex')


class PyRustexContext:
    def __init__(self):
        self.ctx = str(uuid.uuid4())

    def __enter__(self):
        cv.set(self.ctx)
        mutex.add_context(self.ctx)
        return self

    def __exit__(self, type, value, traceback):
        mutex.remove_context(self.ctx)
        cv.set(None)


class PyRustexMutex:
    def __init__(self, name: str):
        self.name = name

    async def __aenter__(self):
        await mutex.acquire_mutex(self.name, cv.get())
        return self

    async def __aexit__(self, type, value, traceback):
        await mutex.release_mutex(self.name, cv.get())


def with_rustex(name: str) -> PyRustexMutex:
    return PyRustexMutex(name)


async def a_main():
    async def drama(i):
        mt = 'test{}'.format(random.randint(0, 3))
        try:
            with PyRustexContext():
                async with with_rustex(mt):
                    print("In a_main in context {} on mtex {}".format(cv.get(), mt))
                    if random.random() < .5:
                        raise ArithmeticError("Dramatic Test Exception")
                    print("Survived mtex {}".format(mt))

        except ArithmeticError as e:
            print("Past exception mtex {}".format(mt))
            pass

    await asyncio.gather(*[drama(i) for i in range(10)])


def main():
    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(a_main())
        loop.close()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

```
