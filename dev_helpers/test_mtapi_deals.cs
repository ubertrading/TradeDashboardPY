using System;
using System.Reflection;
using MtApi5;
class P { 
    static void Main() { 
        foreach(var m in typeof(MtApi5Client).GetMethods()) { 
            if(m.Name.Contains("History") || m.Name.Contains("Deal"))
                Console.WriteLine(m.Name); 
        } 
    } 
}
