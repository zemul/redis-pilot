import * as React from "react";
import { DayPicker } from "react-day-picker";
import "react-day-picker/style.css";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

// 基于 react-day-picker v10 的 shadcn 风格日历。灰色主题、圆角、hover 态与
// dashboard 其余组件一致。单选模式：<Calendar mode="single" selected onSelect />
function Calendar({ className, classNames, showOutsideDays = true, ...props }) {
  return (
    <DayPicker
      showOutsideDays={showOutsideDays}
      className={cn("p-0", className)}
      classNames={{
        months: "flex flex-col sm:flex-row gap-4",
        month: "flex flex-col gap-3",
        month_caption: "flex justify-center pt-1 relative items-center h-8",
        caption_label: "text-sm font-medium text-gray-900",
        nav: "flex items-center gap-1 absolute inset-x-0 top-1 justify-between px-1",
        button_previous: "h-7 w-7 inline-flex items-center justify-center rounded-md border border-gray-200 bg-white text-gray-500 hover:bg-gray-50 transition-colors disabled:opacity-40",
        button_next: "h-7 w-7 inline-flex items-center justify-center rounded-md border border-gray-200 bg-white text-gray-500 hover:bg-gray-50 transition-colors disabled:opacity-40",
        month_grid: "w-full border-collapse",
        weekdays: "flex",
        weekday: "text-gray-400 w-8 h-8 grid place-items-center text-[11px] font-normal",
        week: "flex w-full mt-1",
        day: "w-8 h-8 p-0 text-center text-[13px]",
        day_button: "w-8 h-8 rounded-md inline-flex items-center justify-center font-normal text-gray-700 hover:bg-gray-100 transition-colors aria-selected:opacity-100 outline-none focus-visible:ring-2 focus-visible:ring-gray-900",
        selected: "[&>button]:bg-gray-900 [&>button]:text-white [&>button:hover]:bg-gray-900",
        today: "[&>button]:font-semibold [&>button]:text-gray-900",
        outside: "[&>button]:text-gray-300",
        disabled: "[&>button]:text-gray-300 [&>button]:pointer-events-none",
        hidden: "invisible",
        ...classNames,
      }}
      components={{
        Chevron: ({ orientation }) =>
          orientation === "left" ? <ChevronLeft size={16} /> : <ChevronRight size={16} />,
      }}
      {...props}
    />
  );
}

export { Calendar };
