using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    public record MessageDto
    {
        public string Message { get; set; }
        public int DequeueCount { get; set; }
    }
}
