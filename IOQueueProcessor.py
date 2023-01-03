try:
    import multiprocess as multiprocessing
except:
    import multiprocessing

from queue import Empty, Full
import signal
import time

class IOQueueProcessor():

    def __init__(self, input_class, output_class, **kwargs):

        self.process_count = kwargs.get('process_count', multiprocessing.cpu_count())
        self.all_stop = multiprocessing.Value('i', 0)

        self.input_class = input_class
        self.output_class = output_class
        self.input_queue = multiprocessing.Queue(self.process_count * 10)
        self.output_queue = multiprocessing.Queue(self.process_count * 10)

        self.input_queue_read_cnt = multiprocessing.Value('i', 0)
        self.output_queue_read_cnt = multiprocessing.Value('i', 0)

        self.kwargs = kwargs
        self.process_list = []

    def start_up(self):

        self.process_list.append(multiprocessing.Process(target=self.output_queue_reader, args=(0, self.output_queue, self.output_class), kwargs=self.kwargs))
        for process_number in range(self.process_count - 1):
            self.process_list.append(multiprocessing.Process(target=self.input_queue_reader, args=(process_number+1, self.input_queue, self.output_queue, self.input_class), kwargs=self.kwargs))
        for process in self.process_list:
            process.start()

    def finish_up(self):

        # wait for queues
        try:
            while self.input_queue.qsize() or self.output_queue.qsize():
                print(f"waiting for {self.input_queue.qsize()} input and {self.output_queue.qsize()} output queue records")
                time.sleep(1)
        except: # qsize does not work on mac
            while not self.input_queue.empty() or not self.output_queue.empty():
                if not self.input_queue.empty():
                    print("waiting for input queue to finish")
                elif not self.output_queue.empty():
                    print("waiting for output queue to finish")
                time.sleep(1)

        with self.all_stop.get_lock():
            self.all_stop.value = 1

        start = time.time()
        while time.time() - start <= 15:
            if not any(process.is_alive() for process in self.process_list):
                break
            time.sleep(1)

        for process in self.process_list:
            if process.is_alive():
                print(process.name, 'did not terminate gracefully')
                process.terminate()
            process.join()

        self.input_queue.close()
        self.output_queue.close()

    def queue_read(self, q):
        try:
            return q.get(True, 1)
        except Empty:
            return None

    def queue_write(self, q, msg):
        while True:
            try:
                q.put(msg, True, 1)
            except Full:
                continue
            break

    def input_queue_reader(self, process_number, input_queue, output_queue, function_ref, **kwargs):

        kwargs['process_number'] = process_number
        input_class = function_ref(**kwargs)

        while self.all_stop.value == 0:
            queue_data = self.queue_read(input_queue)
            if queue_data:
                with self.input_queue_read_cnt.get_lock():
                    self.input_queue_read_cnt.value += 1
                result = input_class.run(queue_data)
                if result:
                    self.queue_write(output_queue, result)

        input_class.close()

    def output_queue_reader(self, process_number, output_queue, function_ref, **kwargs):

        kwargs['process_number'] = process_number
        output_class = function_ref(**kwargs)

        while self.all_stop.value == 0:
            queue_data = self.queue_read(output_queue)
            if queue_data:
                with self.output_queue_read_cnt.get_lock():
                    self.output_queue_read_cnt.value += 1
                output_class.run(queue_data)

        output_class.close()

    def process(self, msg):
        self.queue_write(self.input_queue, msg)

    def get_input_queue_read_cnt(self):
        return self.input_queue_read_cnt.value

    def get_output_queue_read_cnt(self):
        return self.output_queue_read_cnt.value


class reader():

    def __init__(self, **kwargs):
        self.process_number = kwargs.get('process_number', -1)
        self.dbo = kwargs['dbo']
        print(f"process {self.process_number} opened {self.dbo}")

    def close(self):
        print(f"process {self.process_number} closed {self.dbo}")

    def run(self, raw_data):
        return f"process {self.process_number} returned {raw_data} mapped!"

class writer():

    def __init__(self, **kwargs):
        self.process_number = kwargs.get('process_number', -1)
        self.output_file_name = kwargs['output_file_name']
        print(f"process {self.process_number} opened {self.output_file_name}")
        self.output_data = []

    def close(self):
        print(f"process {self.process_number} closed {self.output_file_name}")

    def run(self, mapped_data):
        self.output_data.append(mapped_data)
        return f"process {self.process_number} wrote {mapped_data}"


def signal_handler(signal, frame):
    with shut_down.get_lock():
        shut_down.value = 9
    return


if __name__ == '__main__':

    shut_down = multiprocessing.Value('i', 0)
    signal.signal(signal.SIGINT, signal_handler)

    io_queue_processor = IOQueueProcessor(reader, writer, dbo='childbo', output_file_name='outputfile.json')
    print(f"\nstarting {io_queue_processor.process_count} processes\n")
    io_queue_processor.start_up()

    cntr = 0
    for i in range(10):
        data = f"data{cntr}"
        print(f"sent {data}")
        cntr += 1
        io_queue_processor.process(data)
        if shut_down.value != 0:
            break

    print('\nfinishing up ...')
    io_queue_processor.finish_up()

    print(f"\n{cntr} records sent, {io_queue_processor.get_input_queue_read_cnt()} actually read, {io_queue_processor.get_output_queue_read_cnt()} actually written\n")


