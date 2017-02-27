#!/usr/bin/python
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import re
import optparse
import os.path
import random

class VersionInfo:
    '''Just a container for some information.'''
    version = '0.0.1'
    name = 'Wikispaces To Markdown Converter'
    url = 'http://wiki.df.dreamhosters.com/wiki/Wikispaces_to_markdown_Converter'
    author='Daniel Folkinshteyn'

class Starter:
    '''Grabs cli options, and runs the converter on specified files.'''
    def __init__(self):
        self.parse_options()

    def start(self):
        for filepath in self.args:
            wp = WikispacesToMarkdownConverter(filepath, self.options)
            wp.run()

    def parse_options(self):
        '''Read command line options
        '''
        parser = optparse.OptionParser(
                        version=VersionInfo.name + " version " +VersionInfo.version + "\nProject homepage: " + VersionInfo.url,
                        description="This script can convert a Wikispaces-style source page into a Markdown-style source page. For a more detailed usage manual, see the project homepage: " + VersionInfo.url,
                        formatter=optparse.TitledHelpFormatter(),
                        usage="%prog [options] file.creole [file2.creole...]\n")
        parser.add_option("-d", "--debug", action="store_true", dest="debug", help="debug mode (print some extra debug output). [default: %default]")
        parser.add_option("-f", "--file", action="append", dest="file", help="Specify filepath to convert. For multiple files use this option multiple times. [default: %default]")
        parser.add_option("-F", "--filelocation", action="store", dest="filelocation", help="Specify the full/relative URL of directory where files are hosted. This will be used to convert [[file:%s]] links to external links [default: %default]. %s can be used as a placeholder for the linked filename (useful for relative paths)")
        parser.add_option("-I", "--imagelocation", action="store", dest="imagelocation", help="Specify the full/relative URL of directory where images are hosted. This will be used to convert embedded [[image:%s]] to markdown. %s can be used as the placeholder for the image filename [default: %default]")

        parser.set_defaults(debug=False,
                            filelocation='',
                            imagelocation='')

        (self.options, self.args) = parser.parse_args()
        self.options = vars(self.options)
        if self.options['debug']:
            print("Your commandline options:\n", self.options)

        if self.args == []: # where foo is obviously your required option
            parser.print_help()
            exit(1)

class WikispacesToMarkdownConverter:
    '''The actual converter: reads in file, converts, outputs.

    Reference material:
    http://www.markdown.org/wiki/Help:Formatting
    http://www.wikispaces.com/wikitext
    '''
    def __init__(self, filepath, options):
        self.filepath = filepath
        self.options = options

        try:
            if self.options['debug'] != False:
                self.options['debug'] = True
        except KeyError:
                self.options['debug'] = False

        try:
            if self.options['filelocation'] == '':
                self.options['filelocation'] = '%s'
        except KeyError:
                self.options['filelocation'] = '%s'

        try:
            if self.options['imagelocation'] == '':
                self.options['imagelocation'] = '%s'
        except KeyError:
                self.options['imagelocation'] = '%s'

        self.extended_start = False
        self.extended_end = False

        try:
            # the 'rU' mode should convert any \r\n to plain \n.
            self.content = open(filepath, 'rU').read()
        except (OSError, FileNotFoundError):
            self.content = filepath.replace('\r\n', '\n')
            self.filepath = None

    def run(self):
        self.run_regexps()
        return self.write_output()


    def extend_edges(self):
        '''Make sure the content starts and ends with a newline.

        This is to simplify our regexp matching patterns.
        '''
        if not self.content.startswith('\n'):
            self.content = '\n' + self.content
            self.extended_start = True
        if not self.content.endswith('\n\n'):
            self.content = self.content + '\n\n'
            self.extended_end = True

    def restore_edges(self):
        if self.extended_start:
            self.content = self.content[1:]
        if self.extended_end:
            self.content = self.content[:-2]

    def run_regexps(self):
        '''Run some regexps on the source.'''
        self.extend_edges()
        self.extract_verbatim() # take out code and escapes
        self.remove_misc()
        self.parse_ulists()
        self.parse_olists()
        self.parse_headings()
        self.parse_italics()
        self.parse_images()
        self.parse_file_links()
        self.parse_external_links()
        self.parse_underline()
        self.parse_monospaced()
        self.parse_variables()
        # self.parse_includes()
        self.parse_links()
        self.parse_tables()
        self.restore_verbatim() # restore code and escapes
        self.parse_code()
        self.parse_math()
        self.parse_escapes()
        self.restore_edges()

    def remove_misc(self):
        ''' Gives an easy way to detect converter type'''
        self.content = self.content.replace('[[WikiText]]', '[MarkDown]')

        '''remove the [[toc]] since markdown does it by default'''
        self.content = re.sub(r'\n?\[\[toc(\|flat)?\]\]', r'', self.content)

        ''' remove [[#Blah]] named anchors '''
        self.content = re.sub(r'( *)\[\[#.*?\]\]( *)', ' ' * min(1, len(r'\1\2')), self.content)


    def parse_ulists(self):
        def do_replace(matchobj):
            return ('\n  ' * (len(matchobj.group(1)))) + '* '

        self.content = re.sub(r'\n *(\+)\s+', do_replace, self.content)

    def parse_olists(self):
        def do_replace(matchobj):
            return ('\n  ' * (len(matchobj.group(1)))) + '1. '

        """ change ordered lists. This has to occur before parse_headings() """
        self.content = re.sub(r'\n *(\#+)\s+', do_replace, self.content)

    def parse_headings(self):
        def do_replace(matchobj):
            return "\n" + ('#' * min(6, len(matchobj.group(1)))) + " " + matchobj.group(2)
        """ change headings. This has to occur after parse_olists() """
        self.content = re.sub(r'\n *(=+)\s*(.*?)\s*=+', do_replace, self.content)

    def parse_italics(self):
        """change italics from // to * """
        self.content = re.sub(r'(?<!http:)(?<!https:)(?<!ftp:)//', r"*", self.content)

    def parse_external_links(self):
        '''change external link format, and free 'naked' external links.

        external links with labels get single-braces instead of double
        and space instead of pipe as delimiter between url and label

        naked external links (those without label) simply get stripped of
        braces, since that produces the equivalent output in markdown.
        '''
        # change external link format
        self.content = re.sub(r'\[\[@?(https?://[^|\]]*)\|([^\]]*)\]\]', r'[\2](\1)', self.content)
        self.content = re.sub(r'\[\[@?(ftp://[^|\]]*)\|([^\]]*)\]\]', r'[\2](\1)', self.content)

        # free naked external links
        self.content = re.sub(r'\[\[@?(https?://[^|\]]*)\]\]', r'[\1](\1)', self.content)
        self.content = re.sub(r'\[\[@?(ftp://[^|\]]*)\]\]', r'[\1](\1)', self.content)

    def parse_file_links(self):
        '''change file link format to external links.

        file links with labels get the label.
        file links without label get filename as label.
        location of file is specified with cli argument.
        '''
        # change [[file:...]] links to external links
        self.content = re.sub(r'\[\[file:([^|\]]*)\|([^\]]*)\]\]', r'[\2](' + self.options['filelocation'].replace('%s', r'\1') + r')', self.content)
        self.content = re.sub(r'\[\[file:([^|\]]*)\]\]', r'[\1](' + self.options['filelocation'].replace('%s', r'\1') + r')', self.content)

    def parse_links(self):
        # change [[...]] and [[...|...]] links
        # TODO: catch links to other wikis in the form [[Wiki:Page]] and [[Wiki:Page|Linktext]]
        self.content = re.sub(r'\[\[([^|\]]*)\|([^\]]*)\]\]', r'[\2](\1)', self.content)
        self.content = re.sub(r'\[\[([^|\]]*)\]\]', r'[\1](\1)', self.content)

    def parse_underline(self):
        """change underline from __ to _ """
        self.content = re.sub(r'(?s)__(.*?)__', r'_\1_', self.content)

    def parse_monospaced(self):
        """change monospaced font from {{}} to `` """
        self.content = re.sub(r'(?s){{(.*?)}}', r'`\1`', self.content)

    def parse_variables(self):
        """Parse variables.

        The only variable currently supported is {$page}"""
        self.content = re.sub(r'{\$page}', os.path.basename(self.filepath) if not self.filepath is None else '', self.content)

    def parse_includes(self):
        # TODO
        """change includes from [[include...]] to {{}}"""
        self.content = re.sub(r'\[\[include page="([^"]*?)"[^\]]*?\]\]', r'{{:\1}}', self.content)

    def parse_code(self):
        '''convert the [[code]] tags to <pre> tags.

        by default markdown doesn't support code highlighting, so that info
        is lost in conversion.

        there are markdown extensions that do support it, such as GeSHi,
        but they are not included in the default install.

        maybe will add optional support for that with an extra cli option.
        '''
        def code_replace(matchobj):
            code = matchobj.group(2)
            if matchobj.group(1):
                lang = re.sub(r' +format="(.*?)"', r'\1', matchobj.group(1)).lower()
            else:
                lang = ''
            if self.options['debug']:
                print(code)
            return '```' + lang + "\n" + code + "\n```\n"
        self.content = re.sub(r'(?s)\[\[code( +format=".*?")?\]\](.*?)\[\[code\]\]', code_replace, self.content)

    def parse_math(self):
        '''convert the [[math]] tags to <math> tags.'''
        def math_replace(matchobj):
            code = matchobj.group(2)
            if self.options['debug']:
                print(code)
            return '<math>' + code + '</math>'
        self.content = re.sub(r'(?s)\[\[math( +format=".*?")?\]\](.*?)\[\[math\]\]', math_replace, self.content)

    def parse_images(self):
        '''convert [[image:...]] tags to [[File:...]] tags.

        various image attributes are supported:
        align, width, height, caption, link.

        reference material:
        http://www.markdown.org/wiki/Help:Images
        http://www.wikispaces.com/image+tags
        '''
        def image_parse(matchobj):
            imagetag = matchobj.group(0)[:-2]
            if self.options['debug']:
                print(imagetag)
            image_filename = re.search(r'\[\[image:([^ ]*)', imagetag).group(1)

            try:
                image_width = re.search(r'width="(\d+?)"', imagetag).group(1)
            except AttributeError:
                image_width = None

            try:
                image_height = re.search(r'height="(\d+?)"', imagetag).group(1)
            except AttributeError:
                image_height = None

            try:
                image_align = re.search(r'align="(.*?)"', imagetag).group(1)
            except AttributeError:
                image_align = ''

            try:
                image_comment = re.search(r'caption="(.*?)"', imagetag).group(1)
            except AttributeError:
                image_comment = os.path.basename(image_filename)

            try:
                image_link = re.search(r'link="(.*?)"', imagetag).group(1)
            except AttributeError:
                image_link = ''

            if image_link == '':
                return '![%s](%s)' % (image_comment, self.options['imagelocation'].replace('%s', image_filename))
            else:
                return '![%s](%s)(%s)' % (image_comment, self.options['imagelocation'].replace('%s', image_filename), image_link)

        self.content = re.sub(r'\[\[image:[^\]]+\]\]', image_parse, self.content)

    def parse_tables(self):
        '''convert wikispaces tables to markdown tables.'''
        # FIXME: Make more robust, eg. by getting number of columns from 1st row, then readjusting line breaks for the following table rows
        def replace_tables(matchobj):
            atable = matchobj.group(0)
            rows = atable.split('||\n')
            for i, row in enumerate(rows):
                if not row.endswith('||'):
                    rows[i] = row + '||'

            output_table = ''

            rownum = 0
            celltypes = []
            for row in rows:
                output_row = '|'

                cells = re.findall(r'(?s)(?<=\|\|)(.*?)(?=\|\|)', row)
                for cell in cells:
                    if cell.startswith('='):
                        # centered cell
                        cell_type = ':----:'
                        cell = cell[1:]
                    elif cell.startswith('>'):
                        # right aligned
                        cell_type = '----:'
                        cell = cell[1:]
                    elif cell.startswith('~'):
                        # table heading cell
                        cell_type = '----'
                        cell = cell[1:]
                    else:
                        cell_type = '----'

                    if rownum == 0:
                        celltypes += [cell_type]
                    output_row += cell + '|'

                output_table += output_row + '\n'
                if rownum == 0:
                    output_table += '|' + ("|".join(celltypes)) + '|\n'

                rownum += 1

            return output_table

        self.content = re.sub(r'(?s)(?<=\n)([|][|].*?[|][|])(?=\n[^|]|\n[|][^|])',
                replace_tables, self.content)

    def extract_verbatim(self):
        '''Take out sections that should remain unparsed.

        Store them in a dict, leave placeholders in content.
        '''
        self.verbatim_dict = {}
        def replace_verbatim(matchobj):
            while True:
                key = 'verbatim_placeholder_' + str(random.randint(1, 1e15))
                if key not in self.verbatim_dict.keys():
                    break
            self.verbatim_dict[key] = matchobj.group(0)
            return key

        self.content = re.sub(r'(?s)\n?\[\[code( +format=".*?")?\]\](.*?)\[\[code\]\]\n?', replace_verbatim, self.content)
        self.content = re.sub(r'``(.*)``', replace_verbatim, self.content)
        self.content = re.sub(r'(?s)\[\[math( +format=".*?")?\]\](.*?)\[\[math\]\]', replace_verbatim, self.content)

    def restore_verbatim(self):
        '''Restore verbatim sections taken out by extract_verbatim.'''
        for key in self.verbatim_dict.keys():
            self.content = self.content.replace(key, self.verbatim_dict[key])

    def parse_escapes(self):
        '''Replace escapes '``' with '`' tags.'''
        self.content = re.sub(r'``(.*)``', r'`\1`', self.content)

    def write_output(self):
        if not self.filepath is None:

            output_filepath = os.path.join(os.path.dirname(self.filepath),
                                os.path.basename(self.filepath) + '_markdown')

            open(output_filepath, 'w').write(self.content)
        else:
            return self.content


if __name__ == '__main__':
    s = Starter()
    s.start()
