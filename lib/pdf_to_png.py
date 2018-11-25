import sys
import os
from PyPDF2 import PdfFileWriter, PdfFileReader


def crop_pdf_and_convert_to_png(pdf_in,
                                figure_out,
                                left,
                                top,
                                right,
                                bottom):

    pdf = PdfFileReader(open(pdf_in, 'rb'))
    pdf_out = pdf_in + "_tmp.pdf"
    out = PdfFileWriter()
    page = pdf.getPage(0)
    page.mediaBox.upperRight = (page.mediaBox.getUpperRight_x() - right,
                                page.mediaBox.getUpperRight_y() - top)
    page.mediaBox.lowerLeft = (page.mediaBox.getLowerLeft_x() + left,
                               page.mediaBox.getLowerLeft_y() + bottom)
    out.addPage(page)
    pdfout = open(pdf_out, 'wb')
    out.write(pdfout)
    pdfout.close()

    os.system("convert -trim -density 500 %s %s" % (pdf_out, figure_out))
    os.remove(pdf_out)


if __name__ == "__main__":
    crop_pdf_and_convert_to_png("teste.pdf",
                                "tabela.png",
                                0, 20, 0, 20)
