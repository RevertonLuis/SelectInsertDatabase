import datetime
import xlwt
# import xlrd

# Some global parameters
table_title = ("SIPREC PCJ - "
               "PREVISÃO DE PRECIPITAÇÃO (mm) - "
               "MODELO SIMEPAR: %s (UTC)")

boundary_label = "Área de Contribuição"
footnote = "*Acumulado de %s até %s (BRT)"


class WriteXLS():

    def __init__(self, file_name, run, boundaries, table_names):

        self.boundaries = boundaries
        self.table_names = table_names
        self.dates = list(boundaries[list(boundaries.keys())[0]]
                          ["prec_24h_acc"].keys())
        self.dates.sort()
        self.run = run
        self.table_title = (table_title %
                            (self.run.strftime("%d-%m-%Y %H:00")))

        self.file_name = file_name
        self.file = xlwt.Workbook()

        self.font_name = 'Times New Roman'
        self.font_color = 'black'
        self.font_bold = 'off'
        self.font_italic = 'off'

        self.font_wrap = 'off'
        self.font_vertical_alignment = 'centre'
        self.font_horizontal_alignment = 'centre'

        self.cell_fill = 'pattern solid'
        self.cell_background_color = 'fore_colour new_gray'

        # Opcoes: '', thick, thin, double, dashed
        # self.cell_boundary_top = ''
        self.cell_boundary_top = 'top thin'
        self.cell_boundary_bottom = 'bottom thin'
        self.cell_boundary_left = 'left thin'
        self.cell_boundary_right = 'right thin'

    def add_sheet(self, sheet="Sheet 1"):

        self.sheet = self.file.add_sheet(sheet)

    def add_new_colors(self):

        xlwt.add_palette_colour("new_green", 0x21)
        self.file.set_colour_RGB(0x21, 0, 107, 84)

        xlwt.add_palette_colour("new_gray", 0x22)
        self.file.set_colour_RGB(0x22, 240, 240, 240)

        xlwt.add_palette_colour("dark_gray", 0x23)
        self.file.set_colour_RGB(0x23, 200, 200, 200)

        # Adiciona cor amarelo para vento
        xlwt.add_palette_colour("new_yellow", 0x25)
        self.file.set_colour_RGB(0x25, 224, 169, 14)

        xlwt.add_palette_colour("new_red", 0x26)
        self.file.set_colour_RGB(0x26, 255, 0, 0)

    def build_style(self):

        self.style = xlwt.easyxf('font: name %s, color-index %s, '
                                 'bold %s, italic %s; ' % (self.font_name,
                                                           self.font_color,
                                                           self.font_bold,
                                                           self.font_italic) +
                                 'align: wrap %s, vert %s, horiz %s;' %
                                 (self.font_wrap,
                                  self.font_vertical_alignment,
                                  self.font_horizontal_alignment) +
                                 'pattern: ' +
                                 self.cell_fill + ',' +
                                 self.cell_background_color + ';' +
                                 'borders: ' +
                                 self.cell_boundary_top + "," +
                                 self.cell_boundary_bottom + "," +
                                 self.cell_boundary_left + "," +
                                 self.cell_boundary_right + ';')

    def write_table(self):

        # Table title
        self.font_bold = 'on'
        self.cell_background_color = 'fore_colour new_yellow'
        self.build_style()
        self.sheet.write_merge(0, 0,
                               0, len(self.dates),
                               "%s" %
                               (self.table_title),
                               self.style)

        self.font_bold = 'on'
        self.cell_background_color = 'fore_colour new_gray'
        self.build_style()

        # Write the dates on the table
        for date, i in zip(self.dates, range(len(self.dates))):
            ci = i + 1
            date_str = u"%s" % date.strftime("%d-%m-%Y")
            if i == 0:
                date_str = date_str + "*"

            self.sheet.write(1, ci, date_str, self.style)

        # Write the boundary column name
        self.sheet.write(1, 0, u"%s" %
                         boundary_label, self.style)

        for b, l in zip(self.boundaries.keys(),
                        range(len(self.boundaries.keys()))):

            self.font_bold = 'off'
            self.cell_background_color = 'fore_colour new_gray'
            self.build_style()

            # Write the boundary name
            self.sheet.write(l + 2, 0, u"%s" %
                             self.table_names[self.boundaries[b]["b_name"]],
                             self.style)

            for date, i in zip(self.dates, range(len(self.dates))):
                ci = i + 1

                self.font_bold = 'off'
                self.cell_background_color = 'fore_colour white'
                self.build_style()

                self.style.num_format_str = '0.0'
                value = float(self.boundaries[b]["prec_24h_acc"][date])
                self.sheet.write(l + 2, ci, value, self.style)

        # Write the footnote
        self.font_bold = 'off'
        self.cell_background_color = 'fore_colour white'
        self.font_horizontal_alignment = 'right'
        self.build_style()

        date = self.run
        if date.hour != 0:
            date = date - datetime.timedelta(hours=3)

        date_acc = self.dates[1]
        footnote_u = footnote % (date.strftime("%d/%m/%Y %H:00"),
                                 (date_acc).strftime("%d/%m/%Y %H:00"))
        self.sheet.write_merge(len(self.boundaries.keys()) + 2,
                               len(self.boundaries.keys()) + 2,
                               0, len(self.dates),
                               "%s" %
                               (footnote_u),
                               self.style)

    def adjust_cell_widths(self):

        """ Method that adjust the cell widths"""
        self.sheet.col(0).width = 5300
        for c in range(1, len(self.dates)):
            self.sheet.col(c).width = 2700

    def other_properties(self):

        """ Set other properties """

        self.sheet.fit_num_pages = 1

    def save_file(self):

        self.file.save(self.file_name)
