using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace REDFS_ClusterMode
{
    public class PriorityQueue
    {
        Item[] _list;
        int counter = 0;

        public PriorityQueue(int size)
        {
            _list = new Item[size];
        }

        Item get_left_child(int loc)
        {
            return (((loc * 2 + 1) > counter) ? null : _list[loc * 2 + 1]);
        }

        Item get_right_child(int loc)
        {
            return (((loc * 2 + 2) > counter) ? null : _list[loc * 2 + 2]);
        }

        private int compare(Item a, Item b)
        {
            IComparer ic = a.get_comparator();
            return ic.Compare(a, b);
        }

        private void process_upwards(int loc)
        {
            if (loc == 0) return;

            Item c = _list[loc];
            Item p = _list[(loc % 2 != 0) ? (loc / 2) : (loc / 2 - 1)];
            if (compare(c, p) < 0)
            {
                _list[loc] = p;
                _list[(loc % 2 != 0) ? (loc / 2) : (loc / 2 - 1)] = c;
                process_upwards((loc % 2 != 0) ? (loc / 2) : (loc / 2 - 1));
            }
        }

        private void process_downwards(int loc)
        {
            if (loc >= counter) return;

            Item p = _list[loc];
            Item c1 = get_left_child(loc);
            Item c2 = get_right_child(loc);

            if ((p == null) || (c1 == null && c2 == null)) return;

            if (c1 != null && c2 == null)
            {
                if (compare(p, c1) <= 0) return;

                _list[loc] = c1;
                _list[loc * 2 + 1] = p;
                process_downwards(loc * 2 + 1);
            }
            else if (c1 != null && c2 != null)
            {
                if (compare(p, c1) <= 0 && compare(p, c2) <= 0) return;

                if (compare(c1, c2) < 0)
                {
                    Item tmp = _list[loc];
                    _list[loc] = c1;
                    _list[loc * 2 + 1] = tmp;
                    process_downwards(loc * 2 + 1);
                }
                else
                {
                    Item tmp = _list[loc];
                    _list[loc] = c2;
                    _list[loc * 2 + 2] = tmp;
                    process_downwards(loc * 2 + 2);
                }
            }
        }

        public bool enqueue(Item i)
        {
            if (counter == _list.Length) return false;

            int pos = counter;
            _list[counter++] = i;
            process_upwards(pos);
            //Console.WriteLine("enqueued + " + ((fingerprint)i).dbn);
            return true;
        }

        public Item dequeue()
        {
            if (counter == 0) return null;

            Item d = _list[0];
            _list[0] = _list[counter - 1];
            counter--;
            process_downwards(0);
            return d;
        }
    }

    public class SortAPI
    {
        Item _item;

        byte[] _internal_cache = null;
        int _internal_cnt = 0;

        byte[] _internal_cache_op = null;
        int _internal_cnt_op = 0;

        Item[] sort_input_array = null;
        int stack_top = 0;

        void PUSH(Item m)
        {
            sort_input_array[stack_top++] = m;
        }

        Item POP()
        {
            return sort_input_array[--stack_top];
        }

        FileStream inputF, outputF;

        public SortAPI(String inputpath, String outputpath, Item _i)
        {
            _item = _i;
            _internal_cache = new byte[_item.get_size() * 1024];

            inputF = new FileStream(inputpath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            outputF = new FileStream(outputpath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        }

        private void insert_item_output(Item i, bool is_last)
        {
            DEFS.ASSERT(_internal_cache_op != null, "Cache memory is empty");
            DEFS.ASSERT(outputF != null, "output file cannot be null in insert-item-output function");

            if (i != null)
            {
                i.get_bytes(_internal_cache_op, _internal_cnt_op * _item.get_size());
                _internal_cnt_op++;
            }

            if (is_last || _internal_cnt_op == 1024)
            {
                outputF.Write(_internal_cache_op, 0, _internal_cnt_op * _item.get_size());
                _internal_cnt_op = 0;
                outputF.Flush();
            }

            if (is_last)
            {
                DEFS.ASSERT(_internal_cnt_op == 0, "_internal_cnt should be zero bcoz we just flushed");
            }
        }


        void insert_item(Item i, bool is_last)
        {
            DEFS.ASSERT(_internal_cache != null, "Cache memory is empty");
            DEFS.ASSERT(inputF != null, "input file cannot be null in insert-item function");

            i.get_bytes(_internal_cache, _internal_cnt * _item.get_size());
            _internal_cnt++;

            if (is_last || _internal_cnt == 1024)
            {
                //print_contents(_internal_cache, _internal_cnt);
                inputF.Write(_internal_cache, 0, _internal_cnt * _item.get_size());
                _internal_cnt = 0;
                inputF.Flush();
            }

            if (is_last)
            {
                DEFS.ASSERT(_internal_cnt == 0, "_internal_cnt should be zero bcoz we just flushed");
                Console.WriteLine("SORT", "Finised inserting all test_insert_items");
                inputF.Seek(0, SeekOrigin.Begin);
            }
        }

        private void prepare_next_chunk(int cid)
        {

            int csize = (1024 * 1024) * _item.get_size();
            long offset = (long)cid * csize;
            int bytes = ((inputF.Length - offset) < csize) ? (int)(inputF.Length - offset) : csize;
            int num_items = bytes / _item.get_size();

            inputF.Read(_internal_cache, 0, bytes);

            if (num_items < 1024 * 1024)
            {
                Console.WriteLine("SORT", "Processing the last chunk : " + cid);
            }
            else
            {
                Console.WriteLine("SORT", "Processing chunk : " + cid);
            }

            for (int i = 0; i < num_items; i++)
            {
                sort_input_array[i].parse_bytes(_internal_cache, _item.get_size() * i);
                //Console.WriteLine(((fingerprint)sort_input_array[i]).dbn);
            }

            Array.Sort(sort_input_array, 0, num_items, _item.get_comparator());

            for (int i = 0; i < num_items; i++)
            {
                sort_input_array[i].get_bytes(_internal_cache, _item.get_size() * i);
                //Console.WriteLine(((fingerprint)sort_input_array[i]).dbn);
            }
            inputF.Seek(cid * (long)csize, SeekOrigin.Begin);
            inputF.Write(_internal_cache, 0, bytes);

        }

        public void do_chunk_sort()
        {

            sort_input_array = new Item[1024 * 1024];
            for (int i = 0; i < 1024 * 1024; i++)
            {
                sort_input_array[i] = _item.create_new_obj();
            }

            int csize = (1024 * 1024) * _item.get_size();
            int num_chunks = (int)((inputF.Length % csize == 0) ? (inputF.Length / csize) : (inputF.Length / csize + 1));

            _internal_cache = new byte[_item.get_size() * 1024 * 1024];
            //sort all chunks
            for (int i = 0; i < num_chunks; i++)
            {
                prepare_next_chunk(i);
            }
            _internal_cache = null;
            sort_input_array = null;
        }

        public static Boolean VerifyFileIsSorted(string input_file, Item _iWithComparator)
        {
            FileStream fdest = new FileStream(input_file, FileMode.Open);
            IComparer ic = _iWithComparator.get_comparator();

            byte[] buffer = new byte[_iWithComparator.get_size()];
            
            int numItems = (int)(fdest.Length / buffer.Length) - 1;

            //Read first item
            fdest.Read(buffer, 0, buffer.Length);
            Item prev = _iWithComparator.create_new_obj();
            prev.parse_bytes(buffer, 0);

            Item curr = _iWithComparator.create_new_obj();

            while (numItems-- > 0)
            {
                fdest.Read(buffer, 0, buffer.Length);
                curr.parse_bytes(buffer, 0);
                if (ic.Compare(prev, curr) > 0)
                {
                    fdest.Close();
                    return false;
                }
                prev = curr;
            }
            fdest.Close();
            return true;
        }

        private void populate_vector(int vecid, List<Item>[] _veclist, long[] _offset)
        {
            DEFS.ASSERT(_veclist[vecid].Count == 0, "List must be empty before you replenish");
            long start_offset = _offset[vecid];
            long end_offset = (long)(vecid + 1) * (1024 * 1024 * (long)_item.get_size()) - 1;

            if (end_offset > inputF.Length) end_offset = inputF.Length;

            if (end_offset - start_offset >= _item.get_size())
            {
                int num_items = 0;
                if (end_offset - start_offset >= _item.get_size() * 1024)
                {
                    num_items = 1024;
                }
                else
                {
                    num_items = (int)((end_offset - start_offset) / _item.get_size());
                }

                inputF.Seek(start_offset, SeekOrigin.Begin);
                inputF.Read(_internal_cache, 0, num_items * _item.get_size());

                for (int i = 0; i < num_items; i++)
                {
                    Item x = POP();
                    x.parse_bytes(_internal_cache, i * _item.get_size());
                    x.set_cookie(vecid);
                    _veclist[vecid].Add(x);
                }
                _offset[vecid] += _item.get_size() * num_items;
            }
            else
            {
                Console.WriteLine("SORT", "Finished processing vecid = " + vecid);
            }
        }


        public void do_merge_work()
        {
            int csize = (1024 * 1024) * _item.get_size();
            int num_chunks = (int)((inputF.Length % csize == 0) ? (inputF.Length / csize) : (inputF.Length / csize + 1));
            List<Item>[] _veclist = new List<Item>[num_chunks];
            long[] _offset = new long[num_chunks];
            PriorityQueue pq = new PriorityQueue(num_chunks);

            Console.WriteLine("num chunks in merge " + num_chunks);
            sort_input_array = new Item[1024 * (num_chunks + 1)];
            for (int i = 0; i < sort_input_array.Length; i++)
            {
                PUSH(_item.create_new_obj());
            }
            Console.WriteLine("Pushed " + sort_input_array.Length + " items");

            _internal_cache = new byte[_item.get_size() * 1024];
            _internal_cache_op = new byte[_item.get_size() * 1024];

            for (int i = 0; i < num_chunks; i++)
            {
                _veclist[i] = new List<Item>(1024);
                _offset[i] = (long)i * (1024 * 1024 * (long)_item.get_size());
                populate_vector(i, _veclist, _offset);
                pq.enqueue((Item)_veclist[i][0]);
                _veclist[i].RemoveAt(0);
            }

            long itr = num_chunks;
            while (true)
            {
                Item x = pq.dequeue();

                if (x == null)
                {
                    Console.WriteLine("recieved null value in iteration " + itr);
                    break;
                }
                else
                {
                    //Console.WriteLine("->" + ((fingerprint)x).dbn);
                }
                int idx = x.get_cookie();
                if (_veclist[idx].Count == 0)
                {
                    populate_vector(idx, _veclist, _offset);
                }

                if (_veclist[idx].Count > 0)
                {
                    pq.enqueue((Item)_veclist[idx][0]);
                    _veclist[idx].RemoveAt(0);
                }
                itr++;
                insert_item_output(x, false);
                PUSH(x);
            }
            insert_item_output(null, true);
        }

        public void close_streams()
        {
            inputF.Flush();
            inputF.Close();
            outputF.Flush();
            outputF.Close();
        }
    }
}
