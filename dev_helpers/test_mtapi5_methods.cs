using System;
using System.Reflection;
using MtApi5;
class P { static void Main() { foreach(var m in typeof(MtApi5Client).GetMethods()) { Console.WriteLine(m.Name); } } }
