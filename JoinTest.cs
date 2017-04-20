using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Helper
{
    public class JoinTest
    {


        public void leftJoin()
        {
            var objectlist = GetObjects();

            var personlist = objectlist.Item1;
            var addresslist = objectlist.Item2;

            var results = personlist.AsEnumerable().LeftJoin(addresslist.AsEnumerable(),
                                        p => p.IdAddress,   /// PK
                                        a => a.IdAddress,   /// FK)
                                        (p, a) =>           /// Result Collection
                                          new
                                          {
                                              MyPerson = p,
                                              Addresses = a
                                          }).ToList();

        }


        public void RightJoin()
        {
            var objectlist = GetObjects();

            var personlist = objectlist.Item1;
            var addresslist = objectlist.Item2;

            var results = personlist.AsEnumerable().RightJoin(addresslist.AsEnumerable(),
                                        p => p.IdAddress,   /// PK
                                        a => a.IdAddress,   /// FK)
                                        (p, a) =>           /// Result Collection
                                          new
                                          {
                                              MyPerson = p,
                                              Addresses = a
                                          }).ToList();

        }


        public void FullOuterJoin()
        {
            var objectlist = GetObjects();

            var personlist = objectlist.Item1;
            var addresslist = objectlist.Item2;

            var results = personlist.AsEnumerable().FullOuterJoinJoin(addresslist.AsEnumerable(),
                                        p => p.IdAddress,   /// PK
                                        a => a.IdAddress,   /// FK)
                                        (p, a) =>           /// Result Collection
                                          new
                                          {
                                              MyPerson = p,
                                              Addresses = a
                                          }).ToList();

        }


        public void LeftExcludingJoin()
        {
            var objectlist = GetObjects();

            var personlist = objectlist.Item1;
            var addresslist = objectlist.Item2;

            var results = personlist.AsEnumerable().LeftExcludingJoin(addresslist.AsEnumerable(),
                                        p => p.IdAddress,   /// PK
                                        a => a.IdAddress,   /// FK)
                                        (p, a) =>           /// Result Collection
                                          new
                                          {
                                              MyPerson = p,
                                              Addresses = a
                                          }).ToList();

        }

        public void RightExcludingJoin()
        {
            var objectlist = GetObjects();

            var personlist = objectlist.Item1;
            var addresslist = objectlist.Item2;

            var results = personlist.AsEnumerable().RightExcludingJoin(addresslist.AsEnumerable(),
                                        p => p.IdAddress,   /// PK
                                        a => a.IdAddress,   /// FK)
                                        (p, a) =>           /// Result Collection
                                          new
                                          {
                                              MyPerson = p,
                                              Addresses = a
                                          }).ToList();

        }


        public void FullOuterExcludingJoin()
        {
            var objectlist = GetObjects();

            var personlist = objectlist.Item1;
            var addresslist = objectlist.Item2;

            var results = personlist.AsEnumerable().FulltExcludingJoin(addresslist.AsEnumerable(),
                                        p => p.IdAddress,   /// PK
                                        a => a.IdAddress,   /// FK)
                                        (p, a) =>           /// Result Collection
                                          new
                                          {
                                              MyPerson = p,
                                              Addresses = a
                                          }).ToList();

        }


        private Tuple<List<Person>, List<Address>> GetObjects()
        {
            var personlist = new List<Person>()
            {
                new Person() {Age = 2, Born = DateTime.UtcNow, ID = "1", IdAddress = 1, Name = "Test"},
                new Person() {Age = 2, Born = DateTime.UtcNow, ID = "2", IdAddress = 2, Name = "Test"},
                new Person() {Age = 2, Born = DateTime.UtcNow, ID = "3", IdAddress = 3, Name = "Test"}
            };


            var addresslist = new List<Address>()
            {
                new Address() {IdAddress = 1, City = "Pune", Num = 1,Street = "A"},
                  new Address() {IdAddress = 2, City = "Nasik", Num = 2,Street = "B"},
                     new Address() {IdAddress = 4, City = "Nagpur", Num = 4,Street = "D"},
            };


            return new Tuple<List<Person>, List<Address>>(personlist, addresslist);
        }

    }


    public static class JoinHelper
    {

        public static IEnumerable<TResult>
            LeftJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
                IEnumerable<TInner> inner,
                Func<TSource, TKey> pk,
                Func<TInner, TKey> fk,
                Func<TSource, TInner, TResult> result)
        {
            IEnumerable<TResult> _result = Enumerable.Empty<TResult>();

            _result = from s in source
                      join i in inner
                      on pk(s) equals fk(i) into joinData
                      from left in joinData.DefaultIfEmpty()
                      select result(s, left);

            return _result;
        }



        public static IEnumerable<TResult>
                  RightJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
                                                  IEnumerable<TInner> inner,
                                                  Func<TSource, TKey> pk,
                                                  Func<TInner, TKey> fk,
                                                  Func<TSource, TInner, TResult> result)
        {
            IEnumerable<TResult> _result = Enumerable.Empty<TResult>();

            _result = from i in inner
                      join s in source
                      on fk(i) equals pk(s) into joinData
                      from right in joinData.DefaultIfEmpty()
                      select result(right, i);

            return _result;
        }


        public static IEnumerable<TResult>
    FullOuterJoinJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
                                                          IEnumerable<TInner> inner,
                                                          Func<TSource, TKey> pk,
                                                          Func<TInner, TKey> fk,
                                                          Func<TSource, TInner, TResult> result)
        {

            var left = source.LeftJoin(inner, pk, fk, result).ToList();
            var right = source.RightJoin(inner, pk, fk, result).ToList();

            return left.Union(right);
        }


        public static IEnumerable<TResult>
    LeftExcludingJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
                                                          IEnumerable<TInner> inner,
                                                          Func<TSource, TKey> pk,
                                                          Func<TInner, TKey> fk,
                                                          Func<TSource, TInner, TResult> result)
        {
            IEnumerable<TResult> _result = Enumerable.Empty<TResult>();

            _result = from s in source
                      join i in inner
                      on pk(s) equals fk(i) into joinData
                      from left in joinData.DefaultIfEmpty()
                      where left == null
                      select result(s, left);

            return _result;
        }


        public static IEnumerable<TResult>
     RightExcludingJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
                                                        IEnumerable<TInner> inner,
                                                        Func<TSource, TKey> pk,
                                                        Func<TInner, TKey> fk,
                                                        Func<TSource, TInner, TResult> result)
        {
            IEnumerable<TResult> _result = Enumerable.Empty<TResult>();

            _result = from i in inner
                      join s in source
                      on fk(i) equals pk(s) into joinData
                      from right in joinData.DefaultIfEmpty()
                      where right == null
                      select result(right, i);

            return _result;
        }



        public static IEnumerable<TResult>
   FulltExcludingJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
                                                      IEnumerable<TInner> inner,
                                                      Func<TSource, TKey> pk,
                                                      Func<TInner, TKey> fk,
                                                      Func<TSource, TInner, TResult> result)
        {
            var left = source.LeftExcludingJoin(inner, pk, fk, result).ToList();
            var right = source.RightExcludingJoin(inner, pk, fk, result).ToList();

            return left.Union(right);
        } 
    }



    public class Person
    {
        public string ID { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public double Salary { get; set; }
        public DateTime Born { get; set; }
        public int IdAddress { get; set; }
    }

    public class Address
    {
        public int IdAddress { get; set; }
        public string Street { get; set; }
        public int Num { get; set; }
        public string City { get; set; }
    }
}
